import asyncio
import websockets
import json
import time
import redis
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone
from collections import defaultdict
import config
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("websocketTest.log"),
        logging.StreamHandler()
    ]
)

# Initialize Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

binance_stream_types = [
    'depth5@100ms', 'depth5@500ms', 'depth5@1000ms',
    'depth10@100ms', 'depth10@500ms', 'depth10@1000ms',
    'depth20@100ms', 'depth20@500ms', 'depth20@1000ms'
]

symbols = ['ETH']

# Global dictionary to store the latest data for all coins
latest_data = {symbol: {
    'binance': {'bids': defaultdict(float), 'asks': defaultdict(float), 'time': 0},
} for symbol in symbols}

last_process_time = {symbol: 0 for symbol in symbols}

def get_timestamp():
    return int(time.time())

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

def get_current_utc_time_with_ms():
    now = datetime.now(timezone.utc)
    return now.isoformat(timespec='milliseconds')

def round_significant_digits(value, significant_digits):
    if value == 0:
        return 0
    d = Decimal(value)
    rounded_value = d.scaleb(-d.adjusted()).quantize(Decimal(10) ** -significant_digits, rounding=ROUND_HALF_UP).scaleb(d.adjusted())
    return float(rounded_value)

def get_current_time_ms():
    return int(time.time() * 1000)

def get_top_n(order_dict, n=5, reverse=False):
    sorted_orders = sorted(order_dict.items(), key=lambda x: float(x[0]), reverse=reverse)
    return [(float(price), float(quantity)) for price, quantity in sorted_orders[:n]]

rate_limiter = RateLimiter(interval=0.025)

def calculate_impact_price(order_book, imn):
    accumulated_notional = 0.0
    accumulated_quantity = 0.0

    for price, quantity in order_book:
        notional = price * quantity
        accumulated_notional += notional
        accumulated_quantity += quantity

        if accumulated_notional >= imn:
            remaining_notional = imn - (accumulated_notional - notional)
            remaining_quantity = remaining_notional / price
            impact_price = imn / (accumulated_quantity - quantity + remaining_quantity)
            return impact_price

    return None

def process_binance_message(symbol, message, time_diff):
    logging.debug(f"Received Binance message for {symbol}")
    data = json.loads(message)

    if 'data' not in data:
        return

    data = data['data']
    event_time = int(data['E'])

    if event_time > latest_data[symbol]['binance']['time']:
        latest_data[symbol]['binance']['time'] = event_time

        bids = data['b']
        asks = data['a']

        latest_data[symbol]['binance']['bids'] = {float(price): float(quantity) for price, quantity in bids}
        latest_data[symbol]['binance']['asks'] = {float(price): float(quantity) for price, quantity in asks}
        process_data(symbol, time_diff)
    else:
        logging.debug(f"Skipping outdated message for {symbol}")


async def binance_websocket_handler(symbol):
    streams = [f"{symbol.lower()}usdt@{stream_type}" for stream_type in binance_stream_types]
    uri = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
    time_diff = get_binance_server_time_diff()

    while True:
        try:
            async with websockets.connect(uri) as websocket:
                logging.info(f"Connected to Binance WebSocket for {symbol}")

                async for message in websocket:
                    process_binance_message(symbol, message, time_diff)

        except websockets.exceptions.ConnectionClosed:
            logging.warning(f"WebSocket connection closed for {symbol}. Reconnecting...")
        except Exception as e:
            logging.error(f"Error in Binance WebSocket for {symbol}: {e}")
        finally:
            logging.info(f"Reconnecting to Binance WebSocket for {symbol}...")
            await asyncio.sleep(5)


def get_binance_server_time_diff():
    """
    Fetches the current server time from Binance.

    Returns:
    int: The server time in milliseconds.
    """
    response = requests.get('https://api.binance.com/api/v3/time')
    server_time = response.json()['serverTime']
    diff = get_current_time_ms() - server_time
    return diff


def process_data(symbol, time_diff):
    global last_process_time
    current_time = time_diff + get_current_time_ms()
    data_time = latest_data[symbol]['binance']['time']
    time_difference = current_time - last_process_time[symbol]
    last_process_time[symbol] = current_time

    if latest_data[symbol]['binance']['time'] != 0:
        if rate_limiter.should_process(symbol):
            current_time = get_current_time_ms()

            combined_data = {
                'timestamp': current_time,
                'binance': {
                    'time': latest_data[symbol]['binance']['time'],
                    'bids': get_top_n(latest_data[symbol]['binance']['bids'], 5, reverse=True),
                    'asks': get_top_n(latest_data[symbol]['binance']['asks'], 5)
                },
            }

            impact_bid_binance = calculate_impact_price(combined_data['binance']['bids'], 100)
            impact_ask_binance = calculate_impact_price(combined_data['binance']['asks'], 100)

            if all(x is not None for x in [impact_bid_binance, impact_ask_binance]):
                combined_data_impact = {
                    'timestamp': get_current_utc_time_with_ms(),
                    'best_bid_price_binance': combined_data['binance']['bids'][0][0],
                    'best_ask_price_binance': combined_data['binance']['asks'][0][0],
                    'impact_bid_price_binance': round(impact_bid_binance, 7),
                    'impact_ask_price_binance': round(impact_ask_binance, 7),
                    'binance_orderbook': combined_data['binance'],
                    'impact_price_reached': True
                }

                if impact_bid_binance > impact_ask_binance:
                    logging.info(
                        f'Binance {symbol}"s impact bid {impact_bid_binance} is greater than its impact ask {impact_ask_binance} ')

            else:
                combined_data_impact = {
                    'timestamp': get_current_utc_time_with_ms(),
                    'binance_orderbook': combined_data['binance'],
                }

            logging.info(f' {time_difference} | {data_time} | {current_time} | {combined_data_impact}')
        else:
            logging.debug(f"Rate limited: Skipping processing for {symbol}")
    else:
        logging.debug(f"Not enough data to process for {symbol}")

async def main():
    tasks = [binance_websocket_handler(symbol) for symbol in symbols]
    await asyncio.gather(*tasks)

async def run():
    while True:
        try:
            await main()
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            logging.info("Restarting the script...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("Script terminated by user")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        logging.info("Restarting the script...")