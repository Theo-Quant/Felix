import asyncio
import websockets
import json
import hmac
import hashlib
import base64
import time
from datetime import datetime
import logging
from binance.client import Client
import redis
import math
import config

okx_contract_sz = config.OKX_CONTRACT_SZ
step_sizes = config.BINANCE_STEP_SZ
unhedged_amounts = {}

# Configure logging
logger = config.setup_logger('WebsocketHedgeAsynch')
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# Initialize Binance client
binance_client = Client(config.BINANCE_API_KEY, config.BINANCE_SECRET_KEY)


def generate_ws_signature(api_secret, timestamp, method, request_path, body):
    message = f"{timestamp}{method}{request_path}{body}"
    mac = hmac.new(bytes(api_secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod=hashlib.sha256)
    d = mac.digest()
    return base64.b64encode(d)


def initialize_clients():
    binance_client.futures_account()
    logger.info("Binance futures account loaded")


async def okx_websocket():
    url = 'wss://ws.okx.com:8443/ws/v5/private'
    print("Connecting to OKX WebSocket...")
    async with websockets.connect(url) as websocket:
        # Authenticate
        timestamp = str(time.time())
        method = 'GET'
        request_path = '/users/self/verify'
        body = ''
        sign = generate_ws_signature(config.OKX_SECRET_KEY, timestamp, method, request_path, body).decode("utf-8")

        login_params = {
            "op": "login",
            "args": [{
                "apiKey": config.OKX_API_KEY,
                "passphrase": config.OKX_PASSPHRASE,
                "timestamp": timestamp,
                "sign": sign
            }]
        }
        await websocket.send(json.dumps(login_params))
        response = await websocket.recv()
        logger.info(f"Login response: {response}")
        print(f"Login response: {response}")

        # Subscribe to order updates
        subscribe_params = {
            "op": "subscribe",
            "args": [{
                "channel": "orders",
                "instType": "SWAP"
            }]
        }

        await websocket.send(json.dumps(subscribe_params))
        response = await websocket.recv()
        logger.info(f"({datetime.now()}) Subscribe response: {response}")
        print(f"Subscribe response: {response}")

        # Listen for order updates
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                print(f"Order update: {data}")

                # Check if the update indicates a fill
                if "data" in data and data["data"]:
                    for order in data["data"]:
                        if order.get("fillSz") != "0":
                            symbol = order["instId"]
                            side = order["side"]
                            amount = float(order["fillSz"])
                            CliOrdId = order['clOrdId']
                            # Create a task for hedging on Binance
                            logger.info(f'Fill identified at {datetime.now()} for {order["fillSz"]} of {order["instId"]}')
                            await asyncio.create_task(hedge_on_binance(symbol, side, amount, CliOrdId))
            except websockets.ConnectionClosed as e:
                logger.error(f"WebSocket connection closed: {e}")
                print(f"WebSocket connection closed: {e}")
                break
            except Exception as e:
                logger.error(f"Error while processing message: {e}")
                print(f"Error while processing message: {e}")


def round_amount(amount, step_size):
    return round(amount / step_size) * step_size


def round_to_precision(value, precision):
    return round(value, precision)


async def hedge_on_binance(symbol, side, amount, CliOrdId):
    contract_multiple = okx_contract_sz.get(symbol)
    step_size = step_sizes.get(symbol.replace('-USDT-SWAP', 'USDT'))
    if not contract_multiple or not step_size:
        logger.error(f"Missing contract size or step size for symbol: {symbol}")
        print(f"Missing contract size or step size for symbol: {symbol}")
        return

    binance_symbol = symbol.replace('-USDT-SWAP', 'USDT') if symbol.endswith('-USDT-SWAP') else symbol.replace('-', '')
    hedge_side = 'SELL' if side.lower() == 'buy' else 'BUY'

    # Calculate precision based on step size
    precision = int(-math.log10(step_size))

    # Calculate the amount to hedge
    hedge_amount = amount * contract_multiple

    # Update the unhedged amount
    if symbol not in unhedged_amounts:
        unhedged_amounts[symbol] = 0

    if hedge_side == 'SELL':
        unhedged_amounts[symbol] -= hedge_amount
    else:  # hedge_side == 'BUY'
        unhedged_amounts[symbol] += hedge_amount

    logger.info(f"Updated unhedged amount for {symbol}: {round(unhedged_amounts[symbol], 4)}")
    print(f"Updated unhedged amount for {symbol}: {round(unhedged_amounts[symbol], 4)}")

    # Determine the actual amount for the Binance order
    actual_amount = round_to_precision(abs(unhedged_amounts[symbol]), precision)

    if actual_amount < step_size:
        logger.info(
            f"Amount {actual_amount} is less than minimum step size {step_size}. Accumulating for future hedge.")
        print(
            f"Amount {actual_amount} is less than minimum step size {step_size}. Accumulating for future hedge.")
        return

    logger.info(
        f"({datetime.now()}) Hedging on Binance: {binance_symbol}, side: {hedge_side}, amount: {actual_amount}")
    print(
        f"({datetime.now()}) Hedging on Binance: {binance_symbol}, side: {hedge_side}, amount: {actual_amount}")

    def place_order_with_retry(retries=3):
        for attempt in range(retries):
            try:
                order = binance_client.futures_create_order(
                    symbol=binance_symbol,
                    side=hedge_side,
                    type='MARKET',
                    quantity=actual_amount,
                    newClientOrderId=CliOrdId,
                )
                print(f"({datetime.now()}) Binance hedge order filled: {order}")

                # Calculate the remaining unhedged amount
                if hedge_side == 'SELL':
                    unhedged_amounts[symbol] += actual_amount
                else:  # hedge_side == 'BUY'
                    unhedged_amounts[symbol] -= actual_amount

                logger.info(f"Updated unhedged amount for {symbol} after order: {round(unhedged_amounts[symbol], 4)}")
                return True
            except Exception as e:
                logger.error(f"*Error* Attempt {attempt + 1} failed : {e}")
                print(f"*Error* Attempt {attempt + 1} failed : {e}")
                if "Server is currently overloaded" in str(e):
                    logger.error("Server overload detected. Setting pause flag.")
                    print("Server overload detected. Setting pause flag.")
                    redis_client.setex('server_overload_pause', 30, 'true')
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return False

    if not place_order_with_retry():
        logger.error(f"Failed to place hedge order after multiple attempts")
        print(f"Failed to place hedge order after multiple attempts")


def main():
    initialize_clients()
    while True:
        try:
            asyncio.run(okx_websocket())
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            print(f"Error occurred: {e}")
            logger.info("Restarting the script in 5 seconds...")
            print("Restarting the script in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    main()
