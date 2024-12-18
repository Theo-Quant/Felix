import asyncio
import websockets
import json
import ccxt.async_support as ccxt
import logging
from datetime import datetime
import config
from decimal import Decimal, InvalidOperation
import time
import random
import string

# Set up logging
logger = config.setup_logger('HyperliquidBybitHedge')


class HyperliquidBybitHedge:
    def __init__(self, symbols):
        self.symbols = symbols
        self.perp_client = None
        self.spot_client = None
        self.last_ping_time = 0
        self.ping_interval = 30  # Send ping every 30 seconds
        self.init_clients()

    def init_clients(self):
        # Initialize Hyperliquid client
        self.perp_client = ccxt.hyperliquid({
            'apiKey': config.HYPERLIQUID_API_KEY,
            'secret': config.HYPERLIQUID_SECRET_KEY,
            'enableRateLimit': True,
            'privateKey': config.HYPERLIQUID_PRIVATE_KEY,
            'walletAddress': config.HYPERLIQUID_MAIN,
        })

        # Initialize Bybit client
        self.spot_client = ccxt.bybit({
            'apiKey': config.BYBIT_API_KEY_HYPERLIQUIDSUB,
            'secret': config.BYBIT_SECRET_KEY_HYPERLIQUIDSUB,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

    async def init_markets(self):
        await self.perp_client.load_markets()
        await self.spot_client.load_markets()

    async def send_ping(self, websocket):
        try:
            ping_message = {
                "method": "ping"
            }
            await websocket.send(json.dumps(ping_message))
            self.last_ping_time = time.time()
            logger.debug("Ping sent")
        except Exception as e:
            logger.error(f"Error sending ping: {e}")
            raise

    async def handle_pong(self, message):
        if isinstance(message, dict) and message.get('channel') == 'pong':
            logger.debug("Pong received")
            return True
        return False

    async def hyperliquid_websocket(self):
        url = 'wss://api.hyperliquid.xyz/ws'

        async with websockets.connect(url) as websocket:
            # Subscribe to userFills
            subscribe_message = {
                "method": "subscribe",
                "subscription": {
                    "type": "userFills",
                    "user": config.HYPERLIQUID_MAIN
                }
            }

            await websocket.send(json.dumps(subscribe_message))
            logger.info(f"Subscribed to Hyperliquid userFills stream for wallet {config.HYPERLIQUID_MAIN}")

            # Handle subscription confirmation
            response = await websocket.recv()
            logger.info(f"Subscription response: {response}")

            # Initialize ping time
            self.last_ping_time = time.time()

            while True:
                try:
                    # Set up concurrent tasks for receiving messages and maintaining ping
                    receive_task = asyncio.create_task(websocket.recv())
                    ping_wait = self.ping_interval - (time.time() - self.last_ping_time)

                    if ping_wait <= 0:
                        await self.send_ping(websocket)
                        ping_wait = self.ping_interval

                    # Wait for either a message or the ping interval
                    try:
                        message = await asyncio.wait_for(receive_task, timeout=ping_wait)
                        logger.info(f"Received raw message: {message}")
                        data = json.loads(message)

                        # Handle pong responses
                        is_pong = await self.handle_pong(data)
                        if not is_pong:
                            await self.process_hyperliquid_fills(data)

                    except asyncio.TimeoutError:
                        # Timeout means it's time to send a ping
                        await self.send_ping(websocket)

                except websockets.exceptions.ConnectionClosed:
                    logger.error("Hyperliquid WebSocket connection closed. Reconnecting...")
                    break
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse message: {message}")
                except Exception as e:
                    logger.error(f"Error in hyperliquid_websocket: {e}")
                    logger.exception("Full traceback:")
                    break

    async def process_hyperliquid_fills(self, data):
        try:
            logger.info(f"Processing fills data: {data}")

            # Process only non-snapshot fills
            if isinstance(data, dict) and data.get('channel') == 'userFills':
                user_fills = data.get('data', {})

                # Skip if this is a snapshot message
                if user_fills.get('isSnapshot'):
                    logger.info("Skipping snapshot message")
                    return

                fills = user_fills.get('fills', [])
                for fill in fills:
                    coin = fill.get('coin')
                    if coin and coin.upper() in self.symbols:
                        side = fill.get('side')
                        price = float(fill.get('px', 0))
                        size = float(fill.get('sz', 0))
                        cli_ord_id = f"hl_hedge_{int(time.time())}_{random.randint(1000, 9999)}"
                        print(f'Hyperliquid fill detected: {size} {coin} at {price}')
                        await self.hedge_order(coin, side, size, cli_ord_id, price)

        except Exception as e:
            logger.error(f"Error processing Hyperliquid fills: {str(e)}")
            logger.exception("Full traceback:")

    async def hedge_order(self, symbol, side, amount, cli_ord_id, price):
        try:
            # Convert Hyperliquid symbol to Bybit format
            bybit_symbol = f"{symbol}/USDT"

            # Flip the side for hedging
            hedge_side = 'sell' if side.lower() == 'buy' else 'buy'

            # Convert amount to Decimal for precision handling
            dec_amount = Decimal(str(amount))
            hedge_amount = dec_amount.quantize(Decimal('0.00001'))
            float_hedge_amount = float(hedge_amount)

            # Prepare order parameters for Bybit
            order_params = {
                'symbol': bybit_symbol,
                'side': hedge_side,
                'amount': float_hedge_amount,
                'params': {
                    'clientOrderId': cli_ord_id,
                    'timeInForce': 'IOC',  # Immediate-or-Cancel for better fills
                    'reduceOnly': False
                }
            }

            logger.info(f"Placing Bybit hedge order: {order_params}")

            # Place the hedge order
            order = await self.spot_client.create_market_order(**order_params)
            logger.info(f"Bybit hedge order placed: {order}")

        except InvalidOperation as e:
            logger.error(f"Decimal conversion error: {e}")
            logger.error(f"Problematic values - amount: {amount}")
        except ccxt.NetworkError as e:
            logger.error(f"Network error when placing Bybit order: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error when placing Bybit order: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in hedge_order: {e}")
            logger.exception("Full traceback:")

    async def run(self):
        await self.init_markets()
        while True:
            try:
                await self.hyperliquid_websocket()
            except Exception as e:
                logger.error(f"WebSocket connection failed: {e}")
                await asyncio.sleep(5)


async def main():
    print("Welcome to the Hyperliquid-Bybit Hedge Script")

    symbols_input = input("Enter the symbols to monitor (comma-separated, e.g., BTC,ETH): ")
    symbols = [symbol.strip().upper() for symbol in symbols_input.split(',')]

    hedge = HyperliquidBybitHedge(symbols)
    await hedge.run()


if __name__ == "__main__":
    asyncio.run(main())