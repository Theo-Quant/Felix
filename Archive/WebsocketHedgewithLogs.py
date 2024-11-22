import asyncio
import websockets
import json
import hmac
import hashlib
import base64
from datetime import datetime
from binance.client import Client as BinanceClient
import config
import math
from APIHedge import APIHedge
from redis import asyncio as aioredis
from TimeOffset import TimeSync
import time
import sys
import traceback


logger = config.setup_logger('WebsocketHedge')
redis_client = aioredis.from_url("redis://localhost")

ERROR_THRESHOLD = 10
ERROR_WINDOW = 300  # seconds


def log_with_time(message):
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    logger.info(f"{current_time} - {message}")


async def perform_hedge_check():
    API_Hedge = APIHedge()
    await API_Hedge.perform_hedge_check()


async def check_and_update_error_state():
    current_time = time.time()
    last_error_time = float(await redis_client.get('last_error_time') or 0)
    error_count = int(await redis_client.get('error_count') or 0)

    if current_time - last_error_time > ERROR_WINDOW:
        error_count = 1
    else:
        error_count += 1

    await redis_client.set('last_error_time', str(current_time))
    await redis_client.set('error_count', str(error_count))

    if error_count >= ERROR_THRESHOLD:
        await stop_run_bot()
        logger.critical(f"Error threshold reached. Stopping run_bot.py")
    return error_count


async def reset_error_count():
    await redis_client.set('error_count', '0')
    await redis_client.delete('last_error_time')


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
        return math.floor(quantity / step_size) * step_size

    async def process_order_update(self, order):
        await reset_error_count()
        print("order Update: ", order)
        if order.get("fillSz") != "0":
            symbol = order["instId"]
            side = order["side"]
            amount = float(order["fillSz"])
            cli_ord_id = order['clOrdId']
            log_with_time(f'3. Fill identified for {order["fillSz"]} of {order["instId"]}')
            await self.hedge_on_binance(symbol, side, amount, cli_ord_id)

    async def hedge_on_binance(self, symbol, side, amount, cli_ord_id):
        log_with_time("4. Hedge operation initiated")
        binance_symbol = symbol.replace('-USDT-SWAP', 'USDT')
        contract_size = self.okx_contract_sz.get(symbol, 1)
        step_size = self.binance_step_sz.get(symbol, 1)
        hedge_side = 'SELL' if side.lower() == 'buy' else 'BUY'

        hedge_amount = amount * contract_size
        if symbol not in self.unhedged_amounts:
            self.unhedged_amounts[symbol] = 0
        self.unhedged_amounts[symbol] += hedge_amount if hedge_side == 'BUY' else -hedge_amount
        log_with_time(f"5. Hedge calculations completed. Unhedged amount: {self.unhedged_amounts[symbol]}")

        actual_hedge_amount = abs(self.unhedged_amounts[symbol])
        rounded_hedge_amount = self.round_step_size(actual_hedge_amount, step_size)

        if rounded_hedge_amount == 0:
            log_with_time("Rounded hedge amount is 0. Skipping hedge.")
            return

        max_retries = 3
        for attempt in range(max_retries):
            try:
                log_with_time("6. Time synchronization check started")
                timestamp = str(int(self.time_sync.get_adjusted_time() / 1000))
                log_with_time("6. Time synchronization check completed")

                log_with_time("7. Binance order placement started")
                order = self.binance_client.futures_create_order(
                    symbol=binance_symbol,
                    side=hedge_side,
                    type='MARKET',
                    quantity=rounded_hedge_amount,
                    newClientOrderId=cli_ord_id,
                    timestamp=timestamp
                )
                log_with_time("7. Binance order placement completed")

                log_with_time(f"8. Order filled: {order}")
                self.unhedged_amounts[symbol] -= rounded_hedge_amount if hedge_side == 'BUY' else -rounded_hedge_amount
                log_with_time(f"9. Unhedged amount updated: {self.unhedged_amounts[symbol]}")

                log_with_time("10. Final error count reset started")
                await reset_error_count()
                log_with_time("10. Final error count reset completed")
                return

            except Exception as e:
                log_with_time(f"Error in hedge attempt {attempt + 1}: {str(e)}")
                if "Server is currently overloaded" in str(e):
                    await redis_client.setex('server_overload_pause', 30, 'true')
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        log_with_time("Max retries reached, performing hedge check")
                        await perform_hedge_check()
                        error_count = await check_and_update_error_state()
                        log_with_time(f"Error count after hedge check: {error_count}")
                        return
                else:
                    if "Order's notional must be no smaller than" in str(e):
                        log_with_time(f"Notional too small, current unhedged: {self.unhedged_amounts.get(symbol, 0)}")
                        await check_and_update_error_state(error_occurred=False)
                    else:
                        log_with_time(f"Unexpected error, performing hedge check")
                        await perform_hedge_check()
                        config.send_telegram_message(str(e), config.bot_token2, config.chat_id2)
                        error_count = await check_and_update_error_state()
                        log_with_time(f"Error count after unexpected error: {error_count}")
                    return

        log_with_time("11. Post-hedge error state update")
        await check_and_update_error_state(error_occurred=False)

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
                if error_count >= ERROR_THRESHOLD:
                    logger.critical("Error threshold reached. Stopping WebsocketHedge.")
                    break
                await asyncio.sleep(5)


def run_binance_hedge():
    binance_client = BinanceClient(config.BINANCE_API_KEY, config.BINANCE_SECRET_KEY)
    time_sync = TimeSync(interval=30)
    try:
        hedge = WebsocketHedge(binance_client, time_sync)
        asyncio.run(hedge.run())  # We still need to use asyncio.run() here because hedge.run() is async
    except Exception as e:
        logger.error(f"Unexpected error in run_binance_hedge: {e}")
        check_and_update_error_state()
    finally:
        binance_client.close_connection()


def run_with_error_handling():
    while True:
        try:
            run_binance_hedge()  # Direct call, no asyncio.run()
        except KeyboardInterrupt:
            logger.info("WebsocketHedge shutting down due to keyboard interrupt...")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            logger.error(traceback.format_exc())
            logger.info("Restarting WebsocketHedge in 10 seconds...")
            time.sleep(10)


if __name__ == "__main__":
    run_with_error_handling()
