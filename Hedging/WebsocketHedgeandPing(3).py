import asyncio
import websockets
import json
import hmac
import hashlib
import base64
from binance.client import Client as BinanceClient
import config
from APIHedge import APIHedge
from redis import asyncio as aioredis
from TimeOffset import TimeSync
import time
import sys
import traceback
from BinanceActivationPings import PingService
from datetime import datetime
from collections import deque

logger = config.setup_logger('WebsocketHedge(3)')
redis_client = aioredis.from_url("redis://localhost")

ERROR_THRESHOLD = 10
ERROR_WINDOW = 300  # seconds


def log_with_time(message):
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    logger.info(f"{current_time} - {message}")


async def perform_hedge_check():
    API_Hedge = APIHedge(config.HEDGE3)
    await API_Hedge.perform_hedge_check()


async def check_and_update_error_state():
    current_time = int(time.time())

    # 1. Add the current timestamp to Redis
    await redis_client.rpush('error_timestamps', current_time)

    # 2. Retrieve all timestamps from Redis
    error_timestamps = await redis_client.lrange('error_timestamps', 0, -1)
    error_timestamps = list(map(int, error_timestamps))

    # 3. Remove timestamps more than 5 minutes earlier than the current timestamp
    cutoff_time = current_time - ERROR_WINDOW
    recent_timestamps = [ts for ts in error_timestamps if ts > cutoff_time]

    # 4. Update Redis with only the recent timestamps
    await redis_client.delete('error_timestamps')
    if recent_timestamps:
        await redis_client.rpush('error_timestamps', *recent_timestamps)

    # 5. Count the length of all the timestamps
    error_count = len(recent_timestamps)

    # 6. If the total count is over 10, then stop the bot
    if error_count >= ERROR_THRESHOLD:
        await stop_run_bot()
        logger.critical(f"Error threshold reached. Stopping run_bot.py")
    return error_count


async def stop_run_bot():
    await redis_client.set('stop_bot', 'true')
    logger.critical("stop_run_bot flag set to true. HighFrequencyBot should stop soon.")


class WebsocketHedge:
    def __init__(self, binance_client: BinanceClient, time_sync: TimeSync):
        self.binance_client = binance_client
        self.time_sync = time_sync
        self.okx_contract_sz = config.OKX_CONTRACT_SZ
        self.binance_step_sz = config.BINANCE_STEP_SZ
        self.unhedged_amounts = {}
        self.ping_service = PingService(binance_client, time_sync)
        self.ping_service.start()

    @staticmethod
    async def generate_okx_signature(timestamp, method, request_path, body):
        message = f"{timestamp}{method}{request_path}{body}"
        mac = hmac.new(bytes(config.OKX_SECRET_KEY, encoding='utf8'), bytes(message, encoding='utf-8'),
                       digestmod=hashlib.sha256)
        d = mac.digest()
        return base64.b64encode(d)

    @staticmethod
    def round_step_size(quantity, step_size):
        if step_size is None:
            return quantity
        return round(quantity / step_size) * step_size

    async def process_order_update(self, order):
        local_adjusted_time = self.time_sync.get_adjusted_time()
        order_update_time = int(order['uTime'])
        time_lag = (local_adjusted_time - order_update_time)
        if order.get('instId').replace('-USDT-SWAP', '') in config.HEDGE3:
            print(f"Order Update: Time lag - {time_lag} ms - {order}")
            if order.get("fillSz") != "0" and not order['clOrdId'].startswith("TTSpread"):
                symbol = order["instId"]
                side = order["side"]
                amount = float(order["fillSz"])
                cli_ord_id = order['clOrdId']
                log_with_time(f'1. Fill identified for {order["fillSz"]} of {order["instId"]}')
                await self.hedge_on_binance(symbol, side, amount, cli_ord_id)

    async def hedge_on_binance(self, symbol, side, amount, cli_ord_id):
        binance_symbol = symbol.replace('-USDT-SWAP', 'USDT')
        contract_size = self.okx_contract_sz.get(symbol, 1)
        step_size = self.binance_step_sz.get(binance_symbol, 1)
        hedge_side = 'SELL' if side.lower() == 'buy' else 'BUY'

        hedge_amount = round(amount * contract_size, 5)
        if symbol not in self.unhedged_amounts:
            self.unhedged_amounts[symbol] = 0
        self.unhedged_amounts[symbol] += hedge_amount if hedge_side == 'BUY' else -hedge_amount

        actual_hedge_amount = abs(self.unhedged_amounts[symbol])
        # Avoid floating decimals in float by rounding
        rounded_hedge_amount = round(self.round_step_size(actual_hedge_amount, step_size), 4)

        if rounded_hedge_amount < step_size:
            log_with_time(f"Rounded hedge amount - {rounded_hedge_amount} is below the step size - {step_size} . Skipping hedge.")
            return

        max_retries = 3
        for attempt in range(max_retries):
            try:
                timestamp = str(int(self.time_sync.get_adjusted_time() / 1000))
                log_with_time("2. Binance order placement started")
                order = self.binance_client.futures_create_order(
                    symbol=binance_symbol,
                    side=hedge_side,
                    type='MARKET',
                    quantity=rounded_hedge_amount,
                    newClientOrderId=cli_ord_id,
                    timestamp=timestamp
                )
                log_with_time("3. Binance order placement completed")

                log_with_time(f"4. Order filled: {order}")
                self.unhedged_amounts[symbol] -= rounded_hedge_amount if hedge_side == 'BUY' else -rounded_hedge_amount
                log_with_time(f"5. Unhedged amount updated: {self.unhedged_amounts[symbol]}")
                return

            except Exception as e:
                if "Server is currently overloaded" in str(e):
                    log_with_time(f"Error in hedge attempt {attempt + 1}: {str(e)}")
                    await redis_client.setex('server_overload_pause', 30, 'true')
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        log_with_time("Max retries reached, performing hedge check")
                        await perform_hedge_check()
                        error_count = await check_and_update_error_state()
                        log_with_time(f"Error count after hedge check: {error_count}")
                        return
                elif "Margin is insufficient" in str(e):
                    config.send_telegram_message(f'Binance margin depleted - {str(e)}. Bot is set to only exit positions. Please manually transfer funds then turn off the "only Exit" signal.', config.bot_token2, config.chat_id2)
                    await redis_client.set('only_exit', 0)
                else:
                    if "Order's notional must be no smaller than" in str(e):
                        log_with_time(f"Notional too small, current unhedged: {self.unhedged_amounts.get(symbol, 0)}")
                    else:
                        log_with_time(f"Unexpected error, performing hedge check")
                        await perform_hedge_check()
                        config.send_telegram_message(str(e), config.bot_token2, config.chat_id2)
                        error_count = await check_and_update_error_state()
                        log_with_time(f"Error count after unexpected error: {error_count}")
                    return
        await check_and_update_error_state()

    async def okx_websocket(self):
        url = 'wss://ws.okx.com:8443/ws/v5/private'
        async with websockets.connect(url) as websocket:
            adjusted_time = str(int(self.time_sync.get_adjusted_time() / 1000))
            signature = await self.generate_okx_signature(adjusted_time, 'GET', '/users/self/verify', '')

            login_params = {
                "op": "login",
                "args": [{
                    "apiKey": config.OKX_API_KEY,
                    "passphrase": config.OKX_PASSPHRASE,
                    "timestamp": adjusted_time,
                    "sign": signature.decode("utf-8")
                }]
            }
            await websocket.send(json.dumps(login_params))
            response = await websocket.recv()
            logger.info(f"Login response: {response}")

            subscribe_params = {
                "op": "subscribe",
                "args": [{"channel": "orders", "instType": "SWAP"}]
            }
            await websocket.send(json.dumps(subscribe_params))
            response = await websocket.recv()
            print(f"Subscribe response: {response}")

            while True:
                message = await websocket.recv()
                if message is None:
                    logger.warning("Received None from websocket, reconnecting...")
                    break
                data = json.loads(message)
                if "data" in data and data["data"]:
                    for order in data["data"]:
                        await self.process_order_update(order)

    async def run(self):
        while True:
            try:
                await self.okx_websocket()
            except Exception as e:
                logger.error(f"Error in OKX websocket: {e}")
                error_count = await check_and_update_error_state()
                logger.warning(f"Consecutive error count: {error_count}")
                await asyncio.sleep(5)


async def run_binance_hedge():
    binance_client = BinanceClient(config.BINANCE_API_KEY, config.BINANCE_SECRET_KEY)
    time_sync = TimeSync(interval=30)
    try:
        hedge = WebsocketHedge(binance_client, time_sync)
        await hedge.run()
    except Exception as e:
        logger.error(f"Unexpected error in run_binance_hedge: {e}")
        await check_and_update_error_state()
    finally:
        await binance_client.close_connection()


def run_with_error_handling():
    asyncio.run(perform_hedge_check())
    while True:
        try:
            asyncio.run(run_binance_hedge())
        except KeyboardInterrupt:
            logger.info("WebsocketHedge shutting down due to keyboard interrupt...")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            logger.error(traceback.format_exc())
            logger.info("Performing hedge check before restarting...")
            asyncio.run(perform_hedge_check())
            logger.info("Restarting WebsocketHedge in 10 seconds...")
            time.sleep(10)


if __name__ == "__main__":
    run_with_error_handling()
