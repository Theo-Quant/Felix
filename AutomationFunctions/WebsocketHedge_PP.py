"""
Dynamic Websocket Hedge Script
=============================

This script implements a dynamic hedging system for cryptocurrency trading across different exchanges.
It monitors order fills on perpetual futures markets and automatically executes hedge orders on spot
markets in real-time using WebSocket connections.

Key Features:
------------
- Supports multiple exchanges (OKX, Binance, and Gate.io)
- Real-time order monitoring via WebSocket connections
- Automatic hedge order execution
- Dynamic symbol support
- Configurable exchange pairs
- Contract size normalization
- Error handling and automatic reconnection

Usage:
------
1. Set up the configuration file with API keys and contract sizes
2. Run the script
3. Select perpetual and spot exchanges (OKX/Binance/Gate)
4. Enter symbols to monitor
5. The script will:
   - Monitor order fills on the perpetual exchange
   - Execute corresponding hedge orders on the spot exchange
   - Handle reconnections and errors automatically
"""

# TODO not done testing... Make sure the hedging mechanism into GATE Spot actually works.
# Currently it should be fine connecting to OKX or Binance orders websockets and sending out the hedge to gate.
# Unsure if the gate hedge will go through though.

import sys
import math
import hmac
import json
import time
import base64
import asyncio
import hashlib
import logging
import websockets
# import config
from datetime import datetime
import ccxt.async_support as ccxt
import dummy_config as config
from decimal import Decimal, InvalidOperation

from pprint import pprint

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Set up logging
logger = logging.getLogger('DynamicWebsocketHedge')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handlers = [
    logging.StreamHandler(),
    logging.FileHandler('DynamicWebsocketHedge.log')
]
for h in handlers:
    h.setFormatter(formatter)
    logger.addHandler(h)

class DynamicWebsocketHedge:
    def __init__(self, perp1_exchange, perp2_exchange, symbols):
        self.perp1_exchange = perp1_exchange.upper()
        self.perp2_exchange = perp2_exchange.upper()
        self.symbols = symbols
        self.perp1_client = None
        self.perp2_client = None
        self.okx_contract_sz = config.OKX_CONTRACT_SZ
        with open('bybit_min_precision.json', 'r') as f:
            self.bybit_min_precisions = json.load(f)
        self.init_clients()
        self.last_ping_time = time.time()
        self.bybit_unhedge = {}

    def init_clients(self):
        if self.perp1_exchange == 'OKX':
            self.perp1_client = ccxt.okx({
                'apiKey': config.OKX_API_KEY,
                'secret': config.OKX_SECRET_KEY,
                'password': config.OKX_PASSPHRASE,
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'}
            })
        elif self.perp1_exchange == 'BYBIT':
            self.perp1_client = ccxt.bybit({
                'apiKey': config.BYBIT_API_KEY,
                'secret': config.BYBIT_SECRET_KEY,
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'}
            })
        
        if self.perp2_exchange == 'OKX':
            self.perp2_client = ccxt.okx({
                'apiKey': config.OKX_API_KEY,
                'secret': config.OKX_SECRET_KEY,
                'password': config.OKX_PASSPHRASE,
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'}
            })
        elif self.perp2_exchange == 'BYBIT':
            self.perp2_client = ccxt.bybit({
                'apiKey': config.BYBIT_API_KEY,
                'secret': config.BYBIT_SECRET_KEY,
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'}
            })

    async def init_markets(self):
        if self.perp1_exchange:
            await self.perp1_client.load_markets()
        if self.perp1_exchange:
            await self.perp2_client.load_markets()

    @staticmethod
    async def generate_okx_signature(timestamp, method, request_path, body):
        message = f"{timestamp}{method}{request_path}{body}"
        mac = hmac.new(bytes(config.OKX_SECRET_KEY, encoding='utf8'), bytes(message, encoding='utf-8'),
                       digestmod='sha256')
        d = mac.digest()
        return base64.b64encode(d)

    async def bybit_websocket(self):
        url = 'wss://stream.bybit.com/v5/private'
        api_key = config.BYBIT_API_KEY
        api_secret = config.BYBIT_SECRET_KEY
        expires = int((time.time() + 2) * 1000) # Do not expire too fast, otherwise will fail
        # Generate signature.
        signature = str(hmac.new(
            bytes(api_secret, "utf-8"),
            bytes(f"GET/realtime{expires}", "utf-8"), digestmod="sha256"
        ).hexdigest())
        async with websockets.connect(url) as websocket:
            auth_message = {
                "op": "auth",
                "args": [api_key, expires, signature]
            }
            await websocket.send(json.dumps(auth_message))
            response = await websocket.recv()
            logger.info(f"BYBIT Auth response: {response}")

            subscribe_message = {
                "op": "subscribe",
                "args": ["order.linear"]
            }
            await websocket.send(json.dumps(subscribe_message))
            response = await websocket.recv()
            logger.info(f"BYBIT Subscribe response: {response}")

            while True:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if "data" in data:
                        for order in data["data"]:
                            await self.process_bybit_order_update(order)
                except websockets.exceptions.ConnectionClosed:
                    logger.error("WebSocket connection closed. Reconnecting...")
                    break
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse message: {message}")
                except Exception as e:
                    logger.error(f"Error in bybit_websocket: {e}")
                    break

    async def okx_websocket(self):
        # https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-order-channel
        url = 'wss://ws.okx.com:8443/ws/v5/private'
        async with websockets.connect(url) as websocket:
            timestamp = str(int(datetime.now().timestamp()))
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
            logger.info(f"OKX Login response: {response}")

            subscribe_params = {
                "op": "subscribe",
                "args": [{"channel": "orders", "instType": "SWAP"}]
            }
            await websocket.send(json.dumps(subscribe_params))
            response = await websocket.recv()
            logger.info(f"OKX Subscribe response: {response}")

            while True:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if "data" in data:
                        for order in data["data"]:
                            await self.process_okx_order_update(order)
                except websockets.exceptions.ConnectionClosed:
                    logger.error("WebSocket connection closed. Reconnecting...")
                    break
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse message: {message}")
                except Exception as e:
                    logger.error(f"Error in okx_websocket: {e}")
                    break
    
    async def process_bybit_order_update(self, order):
        last_ping_time = await self.activation_ping(self.last_ping_time)
        if last_ping_time is not None:
            self.last_ping_time = last_ping_time
        symbol = order.get('symbol')
        base_symbol = symbol.split('USDT')[0]
        
        if base_symbol in self.symbols:
            print(f"Order Update: {order}")
            fill_amount = float(order.get('qty'))
        
            if fill_amount > 0 and order.get('orderStatus') != 'Cancelled' and order.get('orderStatus') != 'New':
                side = order['side']
                side = side.lower()
                cli_ord_id = order.get('orderLinkId', f"hedge_{datetime.now().timestamp()}")
                # handle the case of market/liquidation order
                if order.get('price') == 'null' or order.get('price') == '':
                    price = 0
                else:
                    price = float(order.get('price'))
                local_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                print(f'{local_time}: Fill identified for {fill_amount} of {symbol}')
                await self.hedge_order(symbol, side, fill_amount, cli_ord_id, price)
        
    async def process_okx_order_update(self, order):
        last_ping_time = await self.activation_ping(self.last_ping_time)
        if last_ping_time is not None:
            self.last_ping_time = last_ping_time
        symbol = order.get('s') or order.get('instId')
        base_symbol = symbol.split('-')[0] if '-' in symbol else symbol.split('/')[0]
        contract_sz = self.okx_contract_sz.get(symbol)
        if base_symbol in self.symbols:
            print(f"Order Update: {order}")
            fill_amount = float(order.get('fillSz', 0))

            if fill_amount > 0:
                # convert fill amount out of contract size
                side = order['side']
                coin_amount = fill_amount * contract_sz
                cli_ord_id = order.get('clOrdId', f"hedge_{datetime.now().timestamp()}")
                # handle the case of market/liquidation order
                if order.get('px') == '':
                    price = 0
                else:
                    price = float(order.get('px', 0))
                if price == 0:
                    print(f'Market order detected for {symbol}. Will not hedge for market orders.')
                    return
                local_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                print(f'{local_time}: Fill identified for {fill_amount} of {symbol}')
                await self.hedge_order(symbol, side, coin_amount, cli_ord_id, price)

    async def hedge_order(self, symbol, side, amount, cli_ord_id, price):
        # if cli_ord_id.startswith("PerpPerpArb") | cli_ord_id.startswith("t-PerpPerpArb"):
        try:
            base_symbol = symbol.replace("-USDT-SWAP", "").replace("USDT", "")
            logger.info(f"Starting hedge_order with symbol: {symbol}, side: {side}, amount: {amount}, cli_ord_id: {cli_ord_id}")

            if self.perp2_exchange == 'OKX':
                perp2_symbol = f"{base_symbol}/USDT:USDT" # Tested, trading Swap Perpetuals on OKX
            elif self.perp2_exchange == 'BYBIT':
                perp2_symbol = f"{base_symbol}/USDT:USDT" # Tested, trading Linear Perpetuals on Bybit

            hedge_side = 'sell' if side.lower() == 'buy' else 'buy'

            # Contract size handling and notional price
            if self.perp2_exchange == 'OKX':
                amount = amount / self.okx_contract_sz.get(f'{base_symbol}-USDT-SWAP')
            elif self.perp2_exchange == 'BYBIT':
                # Future work: after finishing the auto update contract size/precision
                # save the contract size as a json file / redis and load it every [time interval]

                # Add the amount to the unhedged amount, if we should buy then add, if we should sell then subtract
                print(f'Perp2 Symbol in self.bybit_unhedge: {perp2_symbol in self.bybit_unhedge}')
                print(f'Amount Unhedged: {self.bybit_unhedge.get(perp2_symbol, 0)}')
                if perp2_symbol in self.bybit_unhedge:
                    self.bybit_unhedge[perp2_symbol] += amount if hedge_side == 'buy' else -amount
                else:
                    self.bybit_unhedge[perp2_symbol] = amount if hedge_side == 'buy' else -amount
                
                print(f'Unhedged amount before hedging for {perp2_symbol}: {self.bybit_unhedge.get(perp2_symbol, 0)}')

                # If the unhedged amount is 0, no need to hedge
                if self.bybit_unhedge.get(perp2_symbol) == 0:
                    logger.info(f'No need to hegde for {perp2_symbol}, there should be no unhedged position.')
                    return
                
                # Round the amount to the nearest min_precision
                min_precision = self.bybit_min_precisions.get(perp2_symbol, 1.0)
                order_amount = round(self.bybit_unhedge.get(perp2_symbol) / min_precision) * min_precision
                amount = order_amount

                # If the amount is 0, you can't hedge because it's too small
                if amount == 0:
                    logger.info(f'The unhedged position for {perp2_symbol} is too small to hedge in Bybit for now.')
                    logger.info(f'Current unhedged amount: {self.bybit_unhedge.get(perp2_symbol)}, min_precision: {min_precision}')
                    return
                
                # If the amount is negative, it means we need to sell
                hedge_side = 'buy' if amount > 0 else 'sell'

                # We take away the order amount from the unhedged amount
                self.bybit_unhedge[perp2_symbol] -= amount if hedge_side == 'buy' else -amount
                print(f'Final unhedged amount for {perp2_symbol}: {self.bybit_unhedge.get(perp2_symbol, 0)}')

            amount = abs(amount)
            dec_amount = Decimal(amount)
            hedge_amount = dec_amount.quantize(Decimal('0.00001'))
            float_hedge_amount = float(hedge_amount)

            if self.perp2_exchange == 'OKX':
                rounded_hedge_amount = float_hedge_amount
            elif self.perp2_exchange == 'BYBIT':
                rounded_hedge_amount = float_hedge_amount # subject to change

            print(f"{self.perp2_exchange} order placement started")

            if self.perp2_exchange == 'OKX':
                order_params = {
                    'symbol': perp2_symbol,
                    'side': hedge_side,
                    'amount': rounded_hedge_amount,
                    'params': {'clOrdId': cli_ord_id}
                }
                print(f"Order parameters: {order_params}")
            elif self.perp2_exchange == 'BYBIT':
                # add four random digits to the cli_ord_id to avoid duplicate order ids
                cli_ord_id = f"{cli_ord_id}_{str(datetime.now().timestamp()).replace('.', '')[-4:]}"
                order_params = {
                    'symbol': perp2_symbol,
                    'side': hedge_side,
                    'amount': rounded_hedge_amount,
                    'params': {'order_link_id': cli_ord_id}
                }
                print(f"Order parameters: {order_params}")
            order = await self.perp2_client.create_market_order(**order_params)
            local_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            logger.info(f"{local_time}: {self.perp2_exchange} order placement completed")
            logger.info(f"Order filled: {order}")
        except InvalidOperation as e:
            logger.error(f"Decimal conversion error: {e}")
            logger.error(f"Problematic values - amount: {amount}")
        except ccxt.NetworkError as e:
            logger.error(f"Network error when placing {self.perp2_exchange} order: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error when placing {self.perp2_exchange} order: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in hedge_order: {e}")
            logger.exception("Full traceback:")
        # Manual order / liquidation order
        # elif cli_ord_id == '':
        #     pass
    async def activation_ping(self, last_ping_time):
        now = time.time()
        if now - last_ping_time < 15:
            return None
        random_symbol = 'BTC/USDT:USDT'
        last_ping_time = now
        trading_price = (await self.perp2_client.fetch_ticker(random_symbol))['last']
        price = trading_price * 0.7
        if self.perp2_exchange == 'OKX':
            amount = 0.001  # 0.001 is the minimum quantity for OKX for BTC/USDT
        elif self.perp2_exchange == 'BYBIT':
            min_trading_notional = 5
            min_trading_quantity = min_trading_notional / trading_price
            amount = max(min_trading_quantity, 0.001)    # 0.001 is the minimum quantity for Bybit for BTC/USDT
        arg = {
            'symbol': random_symbol,
            'type': 'limit',
            'side': 'buy',
            'amount': amount,
            'price': price,
        }
        order_number = (await self.perp2_client.create_order(**arg))['id']
        logger.info(f'Order placed: {order_number}, with arg: {arg}')
        order_cancelled = await self.perp2_client.cancel_order(order_number, random_symbol)
        logger.info(f'Order cancelled: {order_cancelled}')
        return last_ping_time
    
    async def run(self):
        await self.init_markets()
        while True:
            try:
                if self.perp1_exchange == 'OKX':
                    await self.okx_websocket()
                elif self.perp1_exchange == 'BYBIT':
                    await self.bybit_websocket()
            except Exception as e:
                logger.error(f"Error in OKX Websocket: {e}")
                await asyncio.sleep(5)

    async def cleanup(self):
        if self.perp1_client:
            await self.perp1_client.close()
        if self.perp2_client:
            await self.perp2_client.close()

async def main():
    print("Dynamic Websocket Hedge Script")
    print("\nSupported Exchanges:")
    # for exchange in config.SUPPORT_EXCHANGES:
    #     print(f"- {exchange}")
    print('- OKX')
    print('- BYBIT')

    SUPPORTED_EXCHANGES = config.SUPPORT_EXCHANGES
    exchange_str = "/".join(SUPPORTED_EXCHANGES)

    while True:
        perp1_exchange = input(f"\nEnter the perpetual exchange 1 ({exchange_str}): ").upper()
        if perp1_exchange not in SUPPORTED_EXCHANGES:
            print(f"Invalid perpetual exchange 1 selection. Please choose ({exchange_str}).")
        
        perp2_exchange = input(f"\nEnter the perpetual exchange 2 ({exchange_str}): ").upper()
        if perp2_exchange not in SUPPORTED_EXCHANGES:
            print(f"Invalid perpetual exchange 2 selection. Please choose ({exchange_str}).")

        symbols_input = input("Enter the symbols to monitor (comma-separated, e.g., BTC,ETH): ")
        symbols = [symbol.strip().upper() for symbol in symbols_input.split(',')]
        break

    hedge = DynamicWebsocketHedge(perp1_exchange, perp2_exchange, symbols)
    try:
        await hedge.run()
    finally:
        await hedge.cleanup()
    
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Exiting script...")
        exit(0)