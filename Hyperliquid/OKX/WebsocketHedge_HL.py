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
# Currently it should be fine connecting to OKX or Binance orders websockets and sending out the hedge to gate.
# Unsure if the gate hedge will go through though.

import asyncio
import hashlib
import websockets
import json
import hmac
import base64
import ccxt.async_support as ccxt
import logging
from datetime import datetime
import config
from decimal import Decimal, InvalidOperation
import time
import random
import string



# Set up logging
logger = config.setup_logger('DynamicWebsocketHedge')


class DynamicWebsocketHedge:
    def __init__(self, perp_exchange, spot_exchange, symbols):
        self.perp_exchange = perp_exchange.upper()
        self.spot_exchange = spot_exchange.upper()
        self.symbols = symbols
        self.perp_client = None
        self.spot_client = None
        self.gate_hedger = None  # Add this line
        self.okx_contract_sz = config.OKX_CONTRACT_SZ
        self.binance_step_sz = config.BINANCE_STEP_SZ
        self.init_clients()

    def init_clients(self):
        if self.perp_exchange == 'OKX':
            # OKX client initialization is handled in the websocket connection
            pass
        elif self.perp_exchange == 'BINANCE':
            self.perp_client = ccxt.binance({
                'apiKey': config.BINANCE_API_KEY,
                'secret': config.BINANCE_SECRET_KEY,
                'enableRateLimit': True,
                'options': {'defaultType': 'future'}
            })

        if self.spot_exchange == 'BINANCE':
            self.spot_client = ccxt.binance({
                'apiKey': config.BINANCE_API_KEY,
                'secret': config.BINANCE_SECRET_KEY,
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'}
            })
        elif self.spot_exchange == 'OKX':
            self.spot_client = ccxt.okx({
                'apiKey': config.OKX_API_KEY_HYPERLIQUIDSUB,
                'secret': config.OKX_SECRET_KEY_HYPERLIQUIDSUB,
                'password': config.OKX_PASSPHRASE_HYPERLIQUIDSUB,
                'enableRateLimit': True
            })
        elif self.spot_exchange == 'HYPERLIQUID':
            self.spot_client = ccxt.hyperliquid({
                'apiKey': config.HYPERLIQUID_API_KEY,
                'enableRateLimit': True,
                'walletAddress': config.HYPERLIQUID_SUB_HYPEArb,
                'privateKey': config.HYPERLIQUID_SECRET_KEY,
            })

    async def init_markets(self):
        if self.perp_client:
            await self.perp_client.load_markets()
        if self.spot_exchange != 'GATE':    # We don't need to load Gate markets since we created our own class for Gate.io API
            await self.spot_client.load_markets()

    @staticmethod
    async def generate_okx_signature(timestamp, method, request_path, body):
        message = f"{timestamp}{method}{request_path}{body}"
        mac = hmac.new(bytes(config.OKX_SECRET_KEY_HYPERLIQUIDSUB, encoding='utf8'), bytes(message, encoding='utf-8'),
                       digestmod='sha256')
        d = mac.digest()
        return base64.b64encode(d)

    async def process_okx_order_update(self, order):
        print(order)
        symbol = order.get('s') or order.get('instId')
        base_symbol = symbol.split('-')[0] if '-' in symbol else symbol.split('/')[0]
        contract_sz = self.okx_contract_sz.get(symbol, 1)

        if base_symbol in self.symbols:
            print(f"Order Update: {order}")
            fill_amount = float(order.get('fillSz', 0))

            if fill_amount > 0:
                # convert fill amount out of contract size
                side = order['side']
                coin_amount = float(fill_amount) * float(contract_sz)
                cli_ord_id = order.get('clOrdId', f"hedge_{datetime.now().timestamp()}")

                # Use fillPx or lastPx if px is empty (market orders)
                price = float(order.get('fillPx') or order.get('lastPx') or order.get('avgPx') or 0)

                if price > 0:  # Only proceed if we have a valid price
                    print(f'Fill identified for {fill_amount} of {symbol} at price {price}')
                    await self.hedge_order(symbol, side, coin_amount, cli_ord_id, price)
                else:
                    print(f"Warning: No valid price found for order {order}")

    async def hedge_order(self, symbol, side, amount, cli_ord_id, price):
        # if cli_ord_id.startswith("HLArb") | cli_ord_id.startswith("t-HLArb"):
        try:
            base_symbol = symbol.replace("-USDT-250606", "")
            print(f"Starting hedge_order with symbol: {symbol}, side: {side}, amount: {amount}, cli_ord_id: {cli_ord_id}")

            if self.spot_exchange == 'BINANCE':
                spot_symbol = f"{base_symbol}/USDT"
            elif self.spot_exchange == 'OKX':
                spot_symbol = f"{base_symbol}-USDT"
            elif self.spot_exchange == 'HYPERLIQUID':
                if base_symbol == 'HYPE':
                    spot_symbol = '@107'
                else:
                    spot_symbol = f"{base_symbol}/USDC"
            else:  # GATE
                spot_symbol = f"{base_symbol}_USDT"

            hedge_side = 'sell' if side.lower() == 'buy' else 'buy'

            dec_amount = Decimal(str(amount))
            hedge_amount = dec_amount.quantize(Decimal('0.00001'))
            float_hedge_amount = float(hedge_amount)

            if self.spot_exchange == 'BINANCE':
                rounded_hedge_amount = self.spot_client.amount_to_precision(spot_symbol, float_hedge_amount)
            elif self.spot_exchange == 'GATE':
                rounded_hedge_amount = float_hedge_amount  # Gate.io handles precision internally
            elif self.spot_exchange == 'HYPERLIQUID':
                rounded_hedge_amount = float_hedge_amount
            else:  # OKX
                rounded_hedge_amount = float_hedge_amount

            print(f"{self.spot_exchange} order placement started")

            # Prepare order parameters based on exchange
            if self.spot_exchange == 'BINANCE':
                order_params = {
                    'symbol': spot_symbol,
                    'side': hedge_side,
                    'amount': rounded_hedge_amount,
                    'params': {'newClientOrderId': cli_ord_id}
                }
                print(f"Order parameters: {order_params}")
            elif self.spot_exchange == 'OKX': # OKX
                order_params = {
                    'symbol': spot_symbol,
                    'side': hedge_side,
                    'amount': rounded_hedge_amount,
                    # OKX can't have multiple trades with the same CliOrdID, so add more letters to the end.
                    # This is to account for the partial fills that occasionally happen (Which sends in the same CliOrdID).
                    'params': {'clOrdId': cli_ord_id + ''.join(random.choices(string.ascii_letters + string.digits, k=4))}
                }
                print(f"Order parameters: {order_params}")
            elif self.spot_exchange == 'HYPERLIQUID':
                order_params = {
                    'symbol': spot_symbol,
                    'side': hedge_side,
                    'amount': rounded_hedge_amount,
                    'params': {'clOrdId': cli_ord_id + ''.join(random.choices(string.ascii_letters + string.digits, k=4)),
                               'slippage': 0.05}
                }
                print(f"Order parameters: {order_params}")
            elif self.spot_exchange == 'GATE':
                try:
                    # Gate needs to hedge with marketable limit orders only.
                    if hedge_side == 'buy':
                        marketable_limit_price = float(price)*1.05
                    else:
                        marketable_limit_price = float(price)*0.95

                    order = self.spot_client.place_market_order(
                        symbol=spot_symbol,
                        side=hedge_side,
                        amount=amount,
                        price=marketable_limit_price
                    )
                    print(f"Order parameters: {symbol}, {side}, {amount}")
                    logger.info(f"Gate.io order filled: {order}")
                    return order    # Call the return only for gate since gate doesn't need the rest of the function
                except Exception as e:
                    logger.error(f"Gate.io order failed: {e}")
                    raise

            order = await self.spot_client.create_market_order(**order_params)
            logger.info(f"{self.spot_exchange} order placement completed")
            logger.info(f"Order filled: {order}")

        except InvalidOperation as e:
            logger.error(f"Decimal conversion error: {e}")
            logger.error(f"Problematic values - amount: {amount}")
        except ccxt.NetworkError as e:
            logger.error(f"Network error when placing {self.spot_exchange} order: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error when placing {self.spot_exchange} order: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in hedge_order: {e}")
            logger.exception("Full traceback:")

    async def okx_websocket(self):
        url = 'wss://ws.okx.com:8443/ws/v5/private'
        async with websockets.connect(url) as websocket:
            timestamp = str(int(datetime.now().timestamp()))
            signature = await self.generate_okx_signature(timestamp, 'GET', '/users/self/verify', '')

            login_params = {
                "op": "login",
                "args": [{
                    "apiKey": config.OKX_API_KEY_HYPERLIQUIDSUB,
                    "passphrase": config.OKX_PASSPHRASE_HYPERLIQUIDSUB,
                    "timestamp": timestamp,
                    "sign": signature.decode("utf-8")
                }]
            }
            await websocket.send(json.dumps(login_params))
            response = await websocket.recv()
            print(f"Login response: {response}")  # Debug print

            # Try subscribing to SWAP instead of FUTURES for pre-market futures
            subscribe_params = {
                "op": "subscribe",
                "args": [{
                    "channel": "orders",
                    "instType": "FUTURES",  # Changed from FUTURES to SWAP
                    "instId": "HYPE-USDT-250606"
                }]
            }
            print(f"Sending subscription: {json.dumps(subscribe_params)}")  # Debug print
            await websocket.send(json.dumps(subscribe_params))
            response = await websocket.recv()
            print(f"Subscribe response: {response}")  # Debug print

            print("Starting message loop...")  # Debug print
            while True:
                try:
                    message = await websocket.recv()
                    print(f"Received raw message: {message}")  # Debug print
                    data = json.loads(message)
                    print(f"Parsed data: {data}")  # Debug print
                    if "data" in data:
                        for order in data["data"]:
                            await self.process_okx_order_update(order)
                except websockets.exceptions.ConnectionClosed:
                    print("WebSocket connection closed. Reconnecting...")  # Debug print
                    break
                except json.JSONDecodeError:
                    print(f"Failed to parse message: {message}")  # Debug print
                except Exception as e:
                    print(f"Error in okx_websocket: {e}")  # Debug print
                    break

    async def binance_websocket(self):
        url = 'wss://fstream.binance.com/ws'
        listen_key = await self.get_binance_listen_key()

        async with websockets.connect(f"{url}/{listen_key}") as websocket:
            logger.info("Subscribed to Binance User Data Stream")
            while True:
                try:
                    message = await websocket.recv()
                    logger.debug(f"Received Binance message: {message}")

                    if not message:
                        logger.warning("Received empty message from Binance WebSocket")
                        continue

                    data = json.loads(message)
                    print(f"Parsed Binance data: {data}")
                    await self.process_binance_order_update(data)

                except websockets.exceptions.ConnectionClosed:
                    logger.error("Binance WebSocket connection closed. Reconnecting...")
                    break
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse Binance message: {message}")
                except Exception as e:
                    logger.error(f"Error in binance_websocket: {str(e)}")
                    logger.exception("Full traceback:")
                    break

    async def process_binance_order_update(self, data):
        try:
            event_type = data['e']
            if event_type == 'TRADE_LITE':
                symbol = data['s']
                base_symbol = symbol.replace('USDT', '')

                if base_symbol in self.symbols:
                    fill_amount = float(data['l'])  # Quantity traded
                    fill_price = float(data['L'])  # Last traded price
                    side = data['S']
                    cli_ord_id = data['c']
                    price = data['p']

                    logger.info(f'Binance trade detected (TRADE_LITE): {fill_amount} {base_symbol} at {fill_price}')
                    await self.hedge_order(symbol, side, fill_amount, cli_ord_id, price)

            else:
                logger.debug(f"Received unhandled event type: {event_type}")

        except Exception as e:
            logger.error(f"Error processing Binance update: {str(e)}")
            logger.exception("Full traceback:")

    async def get_binance_listen_key(self):
        try:
            response = await self.perp_client.fapiPrivatePostListenKey()
            return response['listenKey']
        except Exception as e:
            logger.error(f"Error getting Binance listen key: {e}")
            raise

    async def gate_websocket(self):
        url = 'wss://fx-ws.gateio.ws/v4/ws/usdt'  # Gate.io perpetual futures WebSocket URL

        # Generate authentication parameters
        timestamp = str(int(datetime.now().timestamp()))
        signature = self.generate_gate_signature(timestamp)

        async with websockets.connect(url) as websocket:
            # Authenticate
            auth_message = {
                "time": int(timestamp),
                "channel": "futures.orders",
                "event": "subscribe",
                "auth": {
                    "method": "api_key",
                    "KEY": config.GATE_API_KEY,
                    "SIGN": signature
                }
            }

            await websocket.send(json.dumps(auth_message))
            response = await websocket.recv()
            logger.info(f"Gate.io authentication response: {response}")

            # Subscribe to order updates for each symbol
            for symbol in self.symbols:
                subscribe_message = {
                    "time": int(timestamp),
                    "channel": "futures.orders",
                    "event": "subscribe",
                    "payload": [f"{symbol}_USDT"]
                }
                await websocket.send(json.dumps(subscribe_message))
                response = await websocket.recv()
                logger.info(f"Gate.io subscription response for {symbol}: {response}")

            # Process messages
            while True:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)
                    await self.process_gate_order_update(data)
                except websockets.exceptions.ConnectionClosed:
                    logger.error("Gate.io WebSocket connection closed. Reconnecting...")
                    break
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse Gate.io message: {message}")
                except Exception as e:
                    logger.error(f"Error in gate_websocket: {e}")
                    break


    def generate_gate_signature(self, timestamp):
        """Generate signature for Gate.io authentication"""
        message = f"{timestamp}GET/api/v4/futures/usdt/orders"
        signature = hmac.new(
            config.GATE_SECRET_KEY.encode(),
            message.encode(),
            hashlib.sha512
        ).hexdigest()
        return signature

    async def process_gate_order_update(self, data):
        try:
            if 'event' in data and data['event'] == 'update':
                order = data.get('result', {})
                print(order)
                if not order:
                    return
                symbol = order.get('contract', '').replace('_USDT', '')
                if symbol in self.symbols:
                    fill_size = float(order.get('size', 0))
                    if fill_size > 0 and order.get('status') == 'filled':
                        side = order.get('side')
                        client_order_id = order.get('client_order_id', f"hedge_{datetime.now().timestamp()}")

                        logger.info(f'Gate.io fill detected: {fill_size} {symbol} {side}')
                        await self.hedge_order(symbol, side, fill_size, client_order_id)

        except Exception as e:
            logger.error(f"Error processing Gate.io update: {str(e)}")
            logger.exception("Full traceback:")

    async def run(self):
        await self.init_markets()
        while True:
            try:
                if self.perp_exchange == 'OKX':
                    await self.okx_websocket()
                elif self.perp_exchange == 'BINANCE':
                    await self.binance_websocket()
                elif self.perp_exchange == 'GATE':
                    await self.gate_websocket()
            except Exception as e:
                logger.error(f"WebSocket connection failed: {e}")
                await asyncio.sleep(5)


async def main():
    print("Welcome to the Dynamic WebsocketHedge Script")
    print("\nSupported Exchanges:")
    print("1. OKX")
    print("2. BINANCE")
    print("3. GATE")

    while True:
        perp_exchange = input("\nEnter the Perpetual exchange (OKX/BINANCE/GATE): ").upper()
        if perp_exchange not in ['OKX', 'BINANCE', 'GATE']:
            print("Invalid perpetual exchange selection. Please choose OKX, BINANCE, or GATE.")
            continue

        spot_exchange = input("Enter the Spot exchange (OKX/BINANCE/GATE): ").upper()
        if spot_exchange not in ['OKX', 'BINANCE', 'GATE', 'HYPERLIQUID']:
            print("Invalid spot exchange selection. Please choose OKX, BINANCE, or GATE.")
            continue

        symbols_input = input("Enter the symbols to monitor (comma-separated, e.g., BTC,ETH): ")
        symbols = [symbol.strip().upper() for symbol in symbols_input.split(',')]
        break

    hedge = DynamicWebsocketHedge(perp_exchange, spot_exchange, symbols)
    await hedge.run()


if __name__ == "__main__":
    asyncio.run(main())