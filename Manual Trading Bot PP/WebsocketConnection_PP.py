import os
import csv
import json
import time
import redis
import asyncio
import logging
import websockets
from datetime import datetime
import TradingModule_PPConfig as config

# Configuration and constants
EXCHANGE_WS_URLS = config.EXCHANGE_WS_URLS

# Update the STREAM_TYPES dictionary
STREAM_TYPES = config.STREAM_TYPES

# Update the SUPPORT_EXCHANGES list
SUPPORT_EXCHANGES = config.SUPPORT_EXCHANGES

# Initialize logging and Redis client
logging.basicConfig(
    level=logging.ERROR, format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler('websockets.log')
    ]
)
redis_client = redis.Redis(host='localhost', port=6379, db=0)

class ExchangeWebsocket:
    def __init__(self, exchange, market_type, symbol):
        self.exchange = exchange
        self.market_type = market_type
        self.symbol = symbol
        self.ws_url = EXCHANGE_WS_URLS[f"{exchange}{'_' + market_type if market_type else ''}"]
        self.stream_types = STREAM_TYPES[f"{exchange}{'_' + market_type if market_type else ''}"]
        self.spread_calculator = None
        self.last_update_time = 0

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
                        self.spread_calculator.update_orderbook(self.exchange, processed_data)
                else:
                    logging.warning(f"Incomplete orderbook data received from Gate.io for {self.symbol}")

        except json.JSONDecodeError as e:
            logging.error(f"Error decoding Gate.io message: {e}")
        except Exception as e:
            logging.error(f"Error processing Gate.io message: {e}")
class HyperLiquidWebsocket(ExchangeWebsocket):
    async def subscribe(self, websocket): #confirmed
        self.ob = {}
        print(f"{self.exchange} - Subscribing to {self.symbol}")
        for stream_type in self.stream_types:
            self.ob[stream_type] = Orderbook(self.symbol, self.exchange)
            subscribe_message = {
            "method": "subscribe",
                "subscription": {"type": stream_type, "coin": self.symbol}
            }
            await websocket.send(json.dumps(subscribe_message))
    async def process_message(self, message):
        data = json.loads(message)
        if 'data' in data:
            if 'levels' in data["data"]:
                # print(data["data"]["levels"])
                stream_type = 'l2Book'
                levels = data["data"]["levels"]
                bids = levels[0]  # [{'px': '97403', 'sz':'4.6913', 'n':'10'}]
                asks = levels[1]  # [{'px': '97403', 'sz':'4.6913', 'n':'10'}]
                bids = [[float(bid['px']), float(bid['sz'])] for bid in bids[:5]]
                asks = [[float(ask['px']), float(ask['sz'])] for ask in asks[:5]]
                self.ob[stream_type].update(data)
                self.last_update_time = data['data']['time']
                processed_data = {
                    'exchange': 'HYPERLIQUID',
                    'symbol': self.symbol,
                    'market_type': self.market_type,
                    'timestamp': data['data']['time'],
                    'bids': bids,
                    'asks': asks
                }
                print(f"Processed_data for {self.exchange}: {processed_data}")
                self.spread_calculator.update_orderbook(self.exchange, processed_data)


class OKXWebsocket(ExchangeWebsocket):
    async def subscribe(self, websocket):
        self.ob = {}
        for stream_type in self.stream_types:
            self.ob[stream_type] = Orderbook(self.symbol, self.exchange)
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
        print(data)
        if 'data' in data and len(data['data']) > 0:
            stream_type = data['arg']['channel']
            book_data = data['data'][0]
            last_update_time = int(book_data['ts'])
            self.ob[stream_type].update(data)
            if last_update_time < self.last_update_time:
                return
            self.last_update_time = last_update_time  # Timestamp is in milliseconds
            processed_data = {
                'exchange': 'OKX',
                'symbol': self.symbol,
                'market_type': self.market_type,
                'timestamp': last_update_time,
                'bids': [[float(price), float(amount)] for price, amount in list(self.ob[stream_type].bids.items())[:5]],
                'asks': [[float(price), float(amount)] for price, amount in list(self.ob[stream_type].asks.items())[:5]]
            }
            self.spread_calculator.update_orderbook(self.exchange, processed_data)

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
            self.spread_calculator.update_orderbook(self.exchange, processed_data)
        elif 's' in data:  # Perpetual market
            processed_data = {
                'exchange': 'Binance',
                'symbol': self.symbol,
                'market_type': self.market_type,
                'timestamp': data['E'],
                'bids': [[float(price), float(amount)] for price, amount in data['b']],
                'asks': [[float(price), float(amount)] for price, amount in data['a']]
            }
            self.spread_calculator.update_orderbook(self.exchange, processed_data)

class BitgetWebsocket(ExchangeWebsocket):
    async def subscribe(self, websocket):
        for stream_type in self.stream_types:
            subscribe_message = {
                "op": "subscribe",
                "args": [
                    {
                        "instType": "USDT-FUTURES",
                        "channel": stream_type,
                        "instId": f"{self.symbol}USDT"
                    }
                ]
            }
            await websocket.send(json.dumps(subscribe_message))

    async def process_message(self, message):
        data = json.loads(message)
        # Handle subscription confirmation
        if 'event' in data and data['event'] == 'subscribe':
            return
        
        # Handle orderbook snapshot
        if 'action' in data and data['action'] == 'snapshot':
            processed_data = {
                'exchange': 'BITGET',
                'symbol': self.symbol,
                'market_type': self.market_type,
                'timestamp': data['ts'],
                'bids': [[float(price), float(amount)] for price, amount in data['data'][0]['bids'][:5]],
                'asks': [[float(price), float(amount)] for price, amount in data['data'][0]['asks'][:5]]
            }
            if self.spread_calculator:
                self.spread_calculator.update_orderbook(self.exchange, processed_data)

class BybitWebsocket(ExchangeWebsocket):
    async def subscribe(self, websocket):
        self.ob = {}
        for stream_type in self.stream_types:
            self.ob[stream_type] = Orderbook(self.symbol, self.exchange)
        subscribe_message = {
        "op": "subscribe",
        "args": [f'{stream_type}{self.symbol}USDT' for stream_type in self.stream_types]
        }
        await websocket.send(json.dumps(subscribe_message))

    async def process_message(self, message):
        if 'success' in message:
            return
        data = json.loads(message)
        print(data)
        stream = data['topic'].split('.')[0] + '.' + data['topic'].split('.')[1] + '.'
        self.ob[stream].update(data)
        update_time = int(data['ts'])
        if update_time < self.last_update_time:
            return
        self.last_update_time = update_time  # Timestamp is in milliseconds
        processed_data = {
            'exchange': 'BYBIT',
            'symbol': self.symbol,
            'market_type': self.market_type,
            'timestamp': update_time,
            'bids': [[float(price), float(amount)] for price, amount in list(self.ob[stream].bids.items())[:5]],
            'asks': [[float(price), float(amount)] for price, amount in list(self.ob[stream].asks.items())[:5]]
        }
        self.spread_calculator.update_orderbook(self.exchange, processed_data)

class WebsocketFactory:
    @staticmethod
    def create_websocket(exchange, market_type, symbol):
        exchange = exchange.upper()
        if exchange == "OKX":
            return OKXWebsocket(exchange, market_type, symbol)
        elif exchange == "HYPERLIQUID":
            return HyperLiquidWebsocket(exchange, market_type, symbol)
        elif exchange == "BINANCE":
            return BinanceWebsocket(exchange, market_type, symbol)
        elif exchange == "GATE":
            return GateWebsocket(exchange, market_type, symbol)
        elif exchange == "BITGET":
            return BitgetWebsocket(exchange, market_type, symbol)
        elif exchange == "BYBIT":
            return BybitWebsocket(exchange, market_type, symbol)
        else:
            raise ValueError(f"Unsupported exchange: {exchange}")

class CSVLogger:
    def __init__(self, redis_key, max_rows=1000000):
        self.redis_key = redis_key
        self.max_rows = max_rows
        self.current_file = None
        self.current_writer = None
        self.row_count = 0
        self.create_new_file()

    def create_new_file(self):
        if self.current_file:
            self.current_file.close()
        timestamp = datetime.now().strftime("%Y%m%d")
        onedrive_path = "./websockets_data"
        filename = os.path.join(onedrive_path, f"{self.redis_key}_{timestamp}.csv")
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        file_exists = os.path.isfile(filename)
        self.current_file = open(filename, 'a', newline='')  # 'a' for append mode
        self.current_writer = csv.writer(self.current_file)

        # Get the current row count
        self.current_file.seek(0, os.SEEK_END)
        self.row_count = self.current_file.tell() // 100

    def log(self, spread_data):
        if self.row_count >= self.max_rows:
            self.create_new_file()

        row = [
            datetime.now(),
            spread_data['symbol'],
            spread_data['entry_spread'],
            spread_data['exit_spread'],
            spread_data['perp1_best_bid'],
            spread_data['perp1_best_ask'],
            spread_data['perp2_best_bid'],
            spread_data['perp2_best_ask'],
            spread_data['perp1_perp2_time_diff'],
            spread_data['perp1_timestamp'],
            spread_data['perp2_timestamp']
        ]
        self.current_writer.writerow(row)
        self.row_count += 1
        self.current_file.flush()  # Ensure data is written immediately

    def close(self):
        if self.current_file:
            self.current_file.close()

class SpreadCalculator:
    def __init__(self, stream1, stream2, symbol):
        self.stream1 = stream1  # Perp stream
        self.stream2 = stream2  # Perp stream
        self.symbol = symbol
        self.rate_limiter = RateLimiter(interval=0.025)
        self.orderbook_data = {
            'perp1': {'bids': [], 'asks': [], 'timestamp': None},
            'perp2': {'bids': [], 'asks': [], 'timestamp': None}
        }
        self.csv_logger = CSVLogger(f"{self.stream1.exchange}_PERP_{self.stream2.exchange}_PERP_{self.symbol}")

    async def calculate_spread(self):
        while True:
            if self.rate_limiter.should_process(self.symbol):
                spread_data = self.calculate_spread_data()
                if spread_data:
                    self.push_to_redis(spread_data)
            await asyncio.sleep(0.01)

    def update_orderbook(self, exchange, data):
        if exchange == self.stream1.exchange:
            market_type = 'perp1'
        elif exchange == self.stream2.exchange:
            market_type = 'perp2'
        self.orderbook_data[market_type.lower()] = {
            'bids': data['bids'],
            'asks': data['asks'],
            'timestamp': data['timestamp']
        }

    def calculate_spread_data(self):
        perp1_data = self.orderbook_data['perp1']
        perp2_data = self.orderbook_data['perp2']

        if not perp1_data['bids'] or not perp2_data['bids']:
            return None

        perp1_best_bid = perp1_data['bids'][0][0]
        perp1_best_ask = perp1_data['asks'][0][0]
        perp2_best_bid = perp2_data['bids'][0][0]
        perp2_best_ask = perp2_data['asks'][0][0]
        perp1_perp2_time_diff = abs(perp1_data['timestamp'] - perp2_data['timestamp'])

        entry_spread = (perp1_best_bid - perp2_best_ask) / perp2_best_ask * 100
        exit_spread = (perp1_best_ask - perp2_best_bid) / perp2_best_bid * 100

        return {
            'symbol': self.symbol,
            'timestamp': max(perp1_data['timestamp'], perp2_data['timestamp']),
            'perp1_exchange': self.stream1.exchange,
            'perp2_exchange': self.stream2.exchange,
            'entry_spread': round(entry_spread, 4),
            'exit_spread': round(exit_spread, 4),
            'perp1_best_bid': perp1_best_bid,
            'perp1_best_ask': perp1_best_ask,
            'perp2_best_bid': perp2_best_bid,
            'perp2_best_ask': perp2_best_ask,
            'perp1_orderbook': perp1_data,
            'perp2_orderbook': perp2_data,
            'perp1_perp2_time_diff': perp1_perp2_time_diff,
            'perp1_timestamp': perp1_data['timestamp'],
            'perp2_timestamp': perp2_data['timestamp']
        }

    def push_to_redis(self, spread_data):
        # redis_key = f"{self.stream1.exchange}_PERP_{self.stream2.exchange}_PERP_{self.symbol}"
        # print(redis_key)
        # redis_client.rpush(redis_key, json.dumps(spread_data))
        # redis_client.ltrim(redis_key, -500, -1)
        print(f"Spread data for {self.symbol}: {spread_data}")
        self.csv_logger.log(spread_data)

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

class Orderbook:
    def __init__(self, symbol, exchange) -> None:
        self.exchange = exchange
        self.symbol = symbol
        self.bids = {}
        self.asks = {}
        self.ts = 0

    def update(self, data):
        update_method = getattr(self, f'update_{self.exchange.lower().replace(".", "")}')
        update_method(data)

    def update_bybit(self, data):
        if int(data['ts']) <= self.ts:
            return
        self.ts = int(data['ts'])
        event = data['type']
        data = data['data']
        if event == 'snapshot':
            self.bids = {float(price): float(amount) for price, amount in data['b']}
            self.asks = {float(price): float(amount) for price, amount in data['a']}
            self.bids = dict(sorted(self.bids.items(), key=lambda item: item[0], reverse=True))
            self.asks = dict(sorted(self.asks.items(), key=lambda item: item[0]))
        elif event == 'delta':
            for bid_order in data['b']:
                price, amount = bid_order
                price = float(price)
                amount = float(amount)
                if amount == 0:
                    self.bids.pop(price, None)
                else:
                    self.bids[price] = amount
            for ask_order in data['a']:
                price, amount = ask_order
                price = float(price)
                amount = float(amount)
                if amount == 0:
                    self.asks.pop(price, None)
                else:
                    self.asks[price] = amount
            self.bids = dict(sorted(self.bids.items(), key=lambda item: item[0], reverse=True))
            self.asks = dict(sorted(self.asks.items(), key=lambda item: item[0]))

    def update_okx(self, data):
        if int(data['data'][0]['ts']) <= self.ts:
            return
        if 'action' not in data:
            event = 'snapshot' # books5 stream do not have action field
        else:
            event = data['action']
        data = data['data'][0]
        self.ts = int(data['ts'])

        if event == 'snapshot':
            self.bids = {float(price): float(amount) for price, amount, _, _ in data['bids']}
            self.asks = {float(price): float(amount) for price, amount, _, _ in data['asks']}
            self.bids = dict(sorted(self.bids.items(), key=lambda item: item[0], reverse=True))
            self.asks = dict(sorted(self.asks.items(), key=lambda item: item[0]))
        elif event == 'update':
            for bid_order in data['bids']:
                price, amount, _, _ = bid_order
                price = float(price)
                amount = float(amount)
                if amount == 0:
                    self.bids.pop(price, None)
                else:
                    self.bids[price] = amount
            for ask_order in data['asks']:
                price, amount, _, _ = ask_order
                price = float(price)
                amount = float(amount)
                if amount == 0:
                    self.asks.pop(price, None)
                else:
                    self.asks[price] = amount
            self.bids = dict(sorted(self.bids.items(), key=lambda item: item[0], reverse=True))
            self.asks = dict(sorted(self.asks.items(), key=lambda item: item[0]))
    def update_hyperliquid(self, data):
        if int(data['data']['time']) <= self.ts:
            return
        data = data['data']
        self.ts = int(data['time'])
        self.bids = {float(bid['px']): float(bid['sz']) for bid in data['levels'][0]}
        self.asks = {float(ask['px']): float(ask['sz']) for ask in data['levels'][1]}
        self.bids = dict(sorted(self.bids.items(), key=lambda item: item[0], reverse=True))
        self.asks = dict(sorted(self.asks.items(), key=lambda item: item[0]))


async def main():
    print("Welcome to the Dynamic Websocket Connection Script")

    exchanges_str = '/'.join(SUPPORT_EXCHANGES)

    while True:
        exchange1 = input(f"Enter the first Perp exchange ({exchanges_str}): ").upper()
        if exchange1 not in SUPPORT_EXCHANGES:
            print(f"Invalid second perpetual exchange. Please choose from: ({exchanges_str})")
            continue

        exchange2 = input(f"Enter the second Perp exchange ({exchanges_str}): ").upper()
        if exchange2 not in SUPPORT_EXCHANGES:
            print(f"Invalid second perpetual exchange. Please choose from: ({exchanges_str})")
            continue

        if exchange1 == exchange2:
            print("Two exchanges cannot be the same. Please choose different exchanges.")
            continue

        symbols_input = input("Enter the symbols to monitor (comma-separated, e.g., BTC,ETH): ")
        symbols = [symbol.strip().upper() for symbol in symbols_input.split(',')]

        break

    stream1_config = (exchange1, 'PERP')
    stream2_config = (exchange2, 'PERP')

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