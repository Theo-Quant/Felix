"""
WebsocketHedge_OS_BP.py

This script implements a real-time hedging system for cryptocurrency trading.
It maintains a WebSocket connection to OKX exchange, listens for order updates,
and immediately hedges filled orders on Binance futures. The system handles
various error scenarios, including server overloads, and manages unhedged
amounts across multiple trading pairs. It uses asynchronous programming for
efficient network operations and includes logging for monitoring and debugging.
"""

import asyncio
import websockets
import json
import hmac
import hashlib
import base64
from datetime import datetime
from binance import AsyncClient
import config
import math
from Hedging.APIHedge import APIHedge
from redis import asyncio as aioredis
import time
import sys
import traceback

logger = config.setup_logger('WebsocketHedge')
redis_client = aioredis.from_url("redis://localhost")

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
    def __init__(self, binance_client: AsyncClient):
        self.binance_client = binance_client
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
        if order.get("fillSz") != "0":
            symbol = order["instId"]
            side = order["side"]
            amount = float(order["fillSz"])
            cli_ord_id = order['clOrdId']
            logger.info(f'Fill identified at {datetime.now()} for {order["fillSz"]} of {order["instId"]}')
            await self.hedge_on_binance(symbol, side, amount, cli_ord_id)

    async def hedge_on_binance(self, symbol, side, amount, cli_ord_id):
        binance_symbol = symbol.replace('-USDT-SWAP', 'USDT')
        contract_size = self.okx_contract_sz.get(symbol, 1)
        step_size = self.binance_step_sz.get(symbol, 1)
        hedge_side = 'SELL' if side.lower() == 'buy' else 'BUY'

        hedge_amount = amount * contract_size

        if symbol not in self.unhedged_amounts:
            self.unhedged_amounts[symbol] = 0

        if hedge_side == 'SELL':
            self.unhedged_amounts[symbol] -= hedge_amount
        else:
            self.unhedged_amounts[symbol] += hedge_amount

        logger.info(f"Updated unhedged amount for {symbol}: {self.unhedged_amounts[symbol]}")

        actual_hedge_amount = abs(self.unhedged_amounts[symbol])
        rounded_hedge_amount = self.round_step_size(actual_hedge_amount, step_size)

        if rounded_hedge_amount == 0:
            logger.info(f"Rounded hedge amount for {symbol} is 0. Accumulating for future hedge.")
            return

        max_retries = 3
        for attempt in range(max_retries):
            try:
                timestamp = str(int(time.time() * 1000))  # Use local system time
                order = await self.binance_client.futures_create_order(
                    symbol=binance_symbol,
                    side=hedge_side,
                    type='MARKET',
                    quantity=rounded_hedge_amount,
                    newClientOrderId=cli_ord_id,
                    timestamp=timestamp
                )
                logger.info(f"Binance hedge order filled: {order}")

                self.unhedged_amounts[symbol] -= rounded_hedge_amount if hedge_side == 'BUY' else -rounded_hedge_amount
                logger.info(f"Updated unhedged amount after hedging for {symbol}: {self.unhedged_amounts[symbol]}")
                await reset_error_count()
                return

            except Exception as e:
                if "Server is currently overloaded" in str(e):
                    logger.error(f"*Error* Attempt {attempt + 1} failed: {e}")
                    logger.error("Server overload detected. Setting pause flag.")
                    await redis_client.setex('server_overload_pause', 30, 'true')
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        logger.error("Max retries reached for server overload, starting error hedge..")
                        await perform_hedge_check()
                        error_count = await check_and_update_error_state()
                        logger.warning(f"Consecutive error count: {error_count}")
                        return
                else:
                    if "Order's notional must be no smaller than" in str(e):
                        logger.info(
                            f"Notional too small, amount added to unhedged. Currently unhedged amount is {self.unhedged_amounts.get(symbol, 0)}")
                        await check_and_update_error_state(error_occurred=False)
                    else:
                        message = f"Unexpected error: {e}, Currently unhedged amount is {self.unhedged_amounts.get(symbol, 0)}"
                        logger.error(message)
                        await perform_hedge_check()
                        config.send_telegram_message(message, config.bot_token2, config.chat_id2)
                        error_count = await check_and_update_error_state()
                        logger.warning(f"Consecutive error count: {error_count}")
                    return

        await check_and_update_error_state(error_occurred=False)

    async def okx_websocket(self):
        url = 'wss://ws.okx.com:8443/ws/v5/private'
        async with websockets.connect(url) as websocket:
            timestamp = str(int(time.time()))  # Use local system time
            signature = await self.generate_okx_signature(timestamp, 'GET', '/users/self/verify', '')

            login_params = {
                "op": "login",
                "args": [{
                    "apiKey": config.OKX_API_KEY,
                    "passphrase": config.OKX_PASSPHRASE,
                    "timestamp": timestamp,
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
                print(f"Order update: {data}")
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

async def run_binance_hedge():
    binance_client = await AsyncClient.create(config.BINANCE_API_KEY, config.BINANCE_SECRET_KEY)
    try:
        hedge = WebsocketHedge(binance_client)
        await hedge.run()
    except Exception as e:
        logger.error(f"Unexpected error in run_binance_hedge: {e}")
        await check_and_update_error_state()
    finally:
        await binance_client.close_connection()
        await redis_client.close()

def run_with_error_handling():
    while True:
        try:
            asyncio.run(run_binance_hedge())
        except KeyboardInterrupt:
            logger.info("WebsocketHedge shutting down due to keyboard interrupt...")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            logger.error(traceback.format_exc())
            logger.info("Restarting WebsocketHedge in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    ERROR_THRESHOLD = 10
    ERROR_WINDOW = 300 # seconds
    run_with_error_handling()