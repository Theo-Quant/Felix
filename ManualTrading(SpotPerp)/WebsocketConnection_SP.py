import asyncio
import websockets
import json
import time
import redis
import logging
from decimal import Decimal
from datetime import datetime, timezone
from collections import defaultdict
import hmac
import base64

# Configuration and constants
EXCHANGE_WS_URLS = {
    "OKX_SPOT": "wss://ws.okx.com:8443/ws/v5/public",
    "OKX_PERP": "wss://ws.okx.com:8443/ws/v5/public",
    "BINANCE_SPOT": "wss://stream.binance.com:9443/ws",
    "BINANCE_PERP": "wss://fstream.binance.com/ws",
    "GATE_SPOT": "wss://api.gateio.ws/ws/v4/",
    "GATE_PERP": "wss://fx-ws.gateio.ws/v4/ws/usdt"
}

# Update the STREAM_TYPES dictionary
STREAM_TYPES = {
    "OKX_SPOT": ["books5"],
    "OKX_PERP": ["books5"],
    "BINANCE_SPOT": ["depth5@100ms"],
    "BINANCE_PERP": ["depth5@100ms"],
    "GATE_SPOT": ["spot.order_book"],
    "GATE_PERP": ["futures.order_book"]
}

# Initialize logging and Redis client
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
redis_client = redis.Redis(host='localhost', port=6379, db=0)


class ExchangeWebsocket:
    def __init__(self, exchange, market_type, symbol):
        self.exchange = exchange
        self.market_type = market_type
        self.symbol = symbol
        self.ws_url = EXCHANGE_WS_URLS[f"{exchange}{'_' + market_type if market_type else ''}"]
        self.stream_types = STREAM_TYPES[f"{exchange}{'_' + market_type if market_type else ''}"]
        self.spread_calculator = None

    async def connect(self):
        while True:
            try:
                async with websockets.connect(self.ws_url) as websocket:
                    await self.subscribe(websocket)
                    async for message in websocket:
                        await self.process_message(message)
            except Exception as e:
                logging.error(f"Error in {self.exchange} {self.market_type} WebSocket: {e}")
                await asyncio.sleep(5)

    async def subscribe(self, websocket):
        pass

    async def process_message(self, message):
        pass


class GateWebsocket(ExchangeWebsocket):
    async def subscribe(self, websocket):
        for stream_type in self.stream_types:
            # Format symbol according to Gate.io requirements
            formatted_symbol = f"{self.symbol}_USDT"
            if self.market_type == 'PERP':
                formatted_symbol = formatted_symbol.upper()
            else:
                formatted_symbol = formatted_symbol.upper()

            subscribe_message = {
                "time": int(time.time()),
                "channel": stream_type,
                "event": "subscribe",
                "payload": [formatted_symbol, "5", "100ms"]  # Level 5 depth, 100ms update interval
            }
            await websocket.send(json.dumps(subscribe_message))
            logging.info(f"Gate.io {self.market_type} subscription sent for {formatted_symbol}")

    async def process_message(self, message):
        try:
            data = json.loads(message)
            # Handle subscription confirmation
            if 'event' in data and data['event'] == 'subscribe':
                return

            # Handle orderbook updates
            if 'result' in data and isinstance(data['result'], dict):
                book_data = data['result']

                # Ensure we have both bids and asks
                if 'bids' in book_data and 'asks' in book_data:
                    processed_data = {
                        'exchange': 'GATE',
                        'symbol': self.symbol,
                        'market_type': self.market_type,
                        'timestamp': int(time.time() * 1000),
                        'bids': [[float(price), float(amount)] for price, amount in book_data['bids'][:5]],
                        'asks': [[float(price), float(amount)] for price, amount in book_data['asks'][:5]]
                    }

                    if self.spread_calculator:
                        self.spread_calculator.update_orderbook(self.market_type, processed_data)
                else:
                    logging.warning(f"Incomplete orderbook data received from Gate.io for {self.symbol}")

        except json.JSONDecodeError as e:
            logging.error(f"Error decoding Gate.io message: {e}")
        except Exception as e:
            logging.error(f"Error processing Gate.io message: {e}")

class OKXWebsocket(ExchangeWebsocket):
    async def subscribe(self, websocket):
        for stream_type in self.stream_types:
            subscribe_message = {
                "op": "subscribe",
                "args": [{
                    "channel": stream_type,
                    "instId": f"{self.symbol}-USDT-SWAP" if self.market_type == 'PERP' else f"{self.symbol}-USDT"
                }]
            }
            await websocket.send(json.dumps(subscribe_message))

    async def process_message(self, message):
        data = json.loads(message)
        if 'data' in data and len(data['data']) > 0:
            book_data = data['data'][0]
            processed_data = {
                'exchange': 'OKX',
                'symbol': self.symbol,
                'market_type': self.market_type,
                'timestamp': int(book_data['ts']),
                'bids': [[float(price), float(amount)] for price, amount, _, _ in book_data['bids']],
                'asks': [[float(price), float(amount)] for price, amount, _, _ in book_data['asks']]
            }
            self.spread_calculator.update_orderbook(self.market_type, processed_data)


class BinanceWebsocket(ExchangeWebsocket):
    async def subscribe(self, websocket):
        symbol = self.symbol.lower()
        print(f"{self.exchange} - Subscribing to {symbol}")
        streams = [f"{symbol}usdt@{stream_type}" for stream_type in self.stream_types]

        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }
        await websocket.send(json.dumps(subscribe_message))

    async def process_message(self, message):
        data = json.loads(message)
        if 'lastUpdateId' in data:  # Spot market
            processed_data = {
                'exchange': 'Binance',
                'symbol': self.symbol,
                'market_type': self.market_type,
                'timestamp': int(time.time() * 1000),
                'bids': [[float(price), float(amount)] for price, amount in data['bids']],
                'asks': [[float(price), float(amount)] for price, amount in data['asks']]
            }
            self.spread_calculator.update_orderbook(self.market_type, processed_data)
        elif 's' in data:  # Perpetual market
            processed_data = {
                'exchange': 'Binance',
                'symbol': self.symbol,
                'market_type': self.market_type,
                'timestamp': data['E'],
                'bids': [[float(price), float(amount)] for price, amount in data['b']],
                'asks': [[float(price), float(amount)] for price, amount in data['a']]
            }
            self.spread_calculator.update_orderbook(self.market_type, processed_data)


class WebsocketFactory:
    @staticmethod
    def create_websocket(exchange, market_type, symbol):
        exchange = exchange.upper()
        if exchange == "OKX":
            return OKXWebsocket(exchange, market_type, symbol)
        elif exchange == "BINANCE":
            return BinanceWebsocket(exchange, market_type, symbol)
        elif exchange == "GATE":
            return GateWebsocket(exchange, market_type, symbol)
        else:
            raise ValueError(f"Unsupported exchange: {exchange}")


class SpreadCalculator:
    def __init__(self, stream1, stream2, symbol):
        self.stream1 = stream1  # Perp stream
        self.stream2 = stream2  # Spot stream
        self.symbol = symbol
        self.rate_limiter = RateLimiter(interval=0.025)
        self.orderbook_data = {
            'perp': {'bids': [], 'asks': [], 'timestamp': None},
            'spot': {'bids': [], 'asks': [], 'timestamp': None}
        }

    async def calculate_spread(self):
        while True:
            if self.rate_limiter.should_process(self.symbol):
                spread_data = self.calculate_spread_data()
                if spread_data:
                    self.push_to_redis(spread_data)
            await asyncio.sleep(0.01)

    def update_orderbook(self, market_type, data):
        self.orderbook_data[market_type.lower()] = {
            'bids': data['bids'],
            'asks': data['asks'],
            'timestamp': data['timestamp']
        }

    def calculate_spread_data(self):
        perp_data = self.orderbook_data['perp']
        spot_data = self.orderbook_data['spot']

        if not perp_data['bids'] or not spot_data['bids']:
            return None

        perp_best_bid = perp_data['bids'][0][0]
        perp_best_ask = perp_data['asks'][0][0]
        spot_best_bid = spot_data['bids'][0][0]
        spot_best_ask = spot_data['asks'][0][0]

        entry_spread = (perp_best_bid - spot_best_ask) / spot_best_ask * 100
        exit_spread = (perp_best_ask - spot_best_bid) / spot_best_bid * 100

        return {
            'symbol': self.symbol,
            'timestamp': max(perp_data['timestamp'], spot_data['timestamp']),
            'perp_exchange': self.stream1.exchange,
            'spot_exchange': self.stream2.exchange,
            'entry_spread': round(entry_spread, 4),
            'exit_spread': round(exit_spread, 4),
            'perp_best_bid': perp_best_bid,
            'perp_best_ask': perp_best_ask,
            'spot_best_bid': spot_best_bid,
            'spot_best_ask': spot_best_ask,
            'perp_orderbook': perp_data,
            'spot_orderbook': spot_data
        }

    def push_to_redis(self, spread_data):
        redis_key = f"{self.stream1.exchange}_PERP_{self.stream2.exchange}_SPOT_{self.symbol}"
        print(redis_key)
        redis_client.rpush(redis_key, json.dumps(spread_data))
        redis_client.ltrim(redis_key, -500, -1)
        print(f"Spread data for {self.symbol}: {spread_data}")


class RateLimiter:
    def __init__(self, interval):
        self.interval = interval
        self.last_check = {}

    def should_process(self, symbol):
        current_time = time.time()
        if symbol not in self.last_check or current_time - self.last_check[symbol] >= self.interval:
            self.last_check[symbol] = current_time
            return True
        return False


async def main():
    print("Welcome to the Dynamic Websocket Connection Script")

    while True:
        exchange1 = input("Enter the Perp exchange (OKX/BINANCE/GATE): ").upper()
        if exchange1 not in ['OKX', 'BINANCE', 'GATE']:
            print("Invalid perpetual exchange. Please choose OKX, BINANCE, or GATE.")
            continue

        exchange2 = input("Enter the Spot exchange (OKX/BINANCE/GATE): ").upper()
        if exchange2 not in ['OKX', 'BINANCE', 'GATE']:
            print("Invalid spot exchange. Please choose OKX, BINANCE, or GATE.")
            continue

        symbols_input = input("Enter the symbols to monitor (comma-separated, e.g., BTC,ETH): ")
        symbols = [symbol.strip().upper() for symbol in symbols_input.split(',')]

        break

    stream1_config = (exchange1, 'PERP')
    stream2_config = (exchange2, 'SPOT')

    tasks = []
    for symbol in symbols:
        try:
            stream1 = WebsocketFactory.create_websocket(*stream1_config, symbol)
            stream2 = WebsocketFactory.create_websocket(*stream2_config, symbol)

            spread_calculator = SpreadCalculator(stream1, stream2, symbol)

            stream1.spread_calculator = spread_calculator
            stream2.spread_calculator = spread_calculator

            tasks.append(stream1.connect())
            tasks.append(stream2.connect())
            tasks.append(spread_calculator.calculate_spread())
        except ValueError as e:
            logging.error(f"Error setting up streams for {symbol}: {e}")
            continue

    if not tasks:
        logging.error("No valid streams could be set up. Exiting.")
        return

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logging.info("Script terminated by user")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())