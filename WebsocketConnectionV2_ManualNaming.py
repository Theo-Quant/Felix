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
import hmac
import base64

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("websocketV2.log"),
        logging.StreamHandler()
    ]
)

OKX_API_KEY = config.OKX_API_KEY
OKX_SECRET_KEY = config.OKX_SECRET_KEY
OKX_PASSPHRASE = config.OKX_PASSPHRASE

# Initialize Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

okx_contract_sz = config.OKX_CONTRACT_SZ

binance_stream_types = [
    'depth5@100ms', 'depth5@500ms', 'depth5@1000ms',
    'depth10@100ms', 'depth10@500ms', 'depth10@1000ms',
    'depth20@100ms', 'depth20@500ms', 'depth20@1000ms'
]

okx_stream_types = ['books5']

symbols = ['CAT']

# Global dictionary to store the latest data and local orderbooks for all coins
latest_data = {symbol: {
    'binance': {'bids': defaultdict(float), 'asks': defaultdict(float), 'time': 0},
    'okx': {
        stream_type: {'bids': [], 'asks': [], 'time': None}
        for stream_type in okx_stream_types
    },
    'local_orderbook': {'bids': [], 'asks': [], 'time': None}
} for symbol in symbols}

last_process_time = {symbol: 0 for symbol in symbols}


def update_local_orderbook(symbol, stream_type, new_data):
    local_ob = latest_data[symbol]['local_orderbook']

    def update_side(side, new_levels):
        current_levels = {price: size for price, size in local_ob[side]}
        for price, size in new_levels:
            if size == 0:
                current_levels.pop(price, None)
            else:
                current_levels[price] = size

        sorted_levels = sorted(current_levels.items(), key=lambda x: -x[0] if side == 'bids' else x[0])
        return sorted_levels[:5]

    if stream_type == 'books5':
        # Replace the entire local orderbook with books5 data
        local_ob['bids'] = new_data['bids']
        local_ob['asks'] = new_data['asks']
    else:
        # Update the local orderbook with new data
        local_ob['bids'] = update_side('bids', new_data['bids'])
        local_ob['asks'] = update_side('asks', new_data['asks'])

    # Ensure we always have 5 levels for both bids and asks
    while len(local_ob['bids']) < 5:
        local_ob['bids'].append((0, 0))
    while len(local_ob['asks']) < 5:
        local_ob['asks'].append((float('inf'), 0))

    local_ob['time'] = new_data['time']

def get_timestamp():
    return int(time.time())

def sign(message, secret_key):
    mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    return base64.b64encode(d)

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
    sorted_orders = sorted(order_dict.items(), key=lambda x: Decimal(x[0]), reverse=reverse)
    return sorted_orders[:n]

rate_limiter = RateLimiter(interval=0.025)  # Updated to 0.01 as requested


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

async def okx_websocket_handler(uri, symbol, stream_type):
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                logging.info(f"Connected to OKX WebSocket for {symbol} ({stream_type})")

                # Generate fresh timestamp and signature immediately before sending login request
                timestamp = str(int(time.time()))
                message = timestamp + 'GET' + '/users/self/verify'
                signature = sign(message, OKX_SECRET_KEY)

                login_params = {
                    "op": "login",
                    "args": [{
                        "apiKey": OKX_API_KEY,
                        "passphrase": OKX_PASSPHRASE,
                        "timestamp": timestamp,
                        "sign": signature.decode("utf-8")
                    }]
                }

                # Send login message
                await websocket.send(json.dumps(login_params))
                login_response = await websocket.recv()
                logging.info(f"Login response: {login_response}")

                # Check if login was successful
                login_data = json.loads(login_response)
                if login_data.get('event') == 'error':
                    logging.error(f"Login failed for {symbol} ({stream_type}): {login_data.get('msg')} (Code: {login_data.get('code')})")
                    raise Exception("Login failed")

                # Send subscribe message
                subscribe_message = {
                    "op": "subscribe",
                    "args": [{"channel": stream_type, "instId": f"{symbol}-USDT-SWAP"}]
                }
                await websocket.send(json.dumps(subscribe_message))

                async for message in websocket:
                    try:
                        if '"event":"ping"' in message or '"event":"pong"' in message:
                            continue
                        process_okx_message(symbol, stream_type, message)
                    except Exception as e:
                        logging.error(f"Error processing message for {symbol} ({stream_type}): {e}")
                        logging.debug(f"Problematic message: {message}")

        except websockets.exceptions.ConnectionClosed:
            logging.warning(f"OKX WebSocket connection closed for {symbol} ({stream_type}). Reconnecting...")
        except Exception as e:
            logging.error(f"Error in OKX WebSocket for {symbol} ({stream_type}): {e}")
        finally:
            logging.info(f"Reconnecting to OKX WebSocket for {symbol} ({stream_type})...")
            await asyncio.sleep(5)

def process_okx_message(symbol, stream_type, message):
    logging.debug(f"Received OKX message for {symbol} ({stream_type}): {message}")
    data = json.loads(message)
    # print(f'OKX | {symbol} | {stream_type} | {data}')

    if 'event' in data:
        if data['event'] == 'subscribe':
            logging.info(f"Successfully subscribed to {stream_type} for {symbol}")
            return
        elif data['event'] == 'error':
            logging.error(f"Error for {symbol} ({stream_type}): {data['msg']} (Code: {data['code']})")
            return

    if 'data' in data:
        book_data = data['data'][0]
        instId = data['arg']['instId'] if 'arg' in data and 'instId' in data['arg'] else book_data.get('instId')
        if not instId:
            logging.warning(f"No instId found in message for {symbol} ({stream_type})")
            return
        contract_size = float(okx_contract_sz.get(instId, 1))

        new_data = {
            'time': int(book_data['ts']),
            'bids': [(float(bid[0]), float(bid[1]) * contract_size) for bid in book_data.get('bids', [])],
            'asks': [(float(ask[0]), float(ask[1]) * contract_size) for ask in book_data.get('asks', [])]
        }

        latest_data[symbol]['okx'][stream_type] = new_data
        update_local_orderbook(symbol, stream_type, new_data)

        # Call process_data after updating the local orderbook
        process_data(symbol)
    else:
        logging.warning(f"Unexpected message structure for {symbol} ({stream_type}): {data}")


# Update the get_top_n function to ensure correct sorting
def get_top_n(order_dict, n=5, reverse=False):
    sorted_orders = sorted(order_dict.items(), key=lambda x: float(x[0]), reverse=reverse)
    return [(float(price), float(quantity)) for price, quantity in sorted_orders[:n]]

# Add a new function to periodically clean up the orderbook
def cleanup_orderbook(symbol):
    current_time = time.time()
    cutoff_time = current_time - 300  # Remove entries older than 5 minutes

    for book_type in ['bids', 'asks']:
        latest_data[symbol]['binance'][book_type] = {
            price: qty for price, qty in latest_data[symbol]['binance'][book_type].items()
            if latest_data[symbol]['binance']['time'] > cutoff_time
        }

def process_binance_message(symbol, message):
    logging.debug(f"Received Binance message for {symbol}")
    data = json.loads(message)
    # print(f'Binance | {symbol} | {data}')

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
        process_data(symbol)
    else:
        logging.debug(f"Skipping outdated message for {symbol}")


async def binance_websocket_handler(symbol):
    # Modified to use 1000CAT instead of CAT for Binance
    binance_symbol = '1000' + symbol if symbol == 'CAT' else symbol
    streams = [f"{binance_symbol.lower()}usdt@{stream_type}" for stream_type in binance_stream_types]
    uri = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

    while True:
        try:
            async with websockets.connect(uri) as websocket:
                logging.info(f"Connected to Binance WebSocket for {binance_symbol}")

                async for message in websocket:
                    # Process message with original symbol (CAT) for consistency
                    process_binance_message(symbol, message)

        except websockets.exceptions.ConnectionClosed:
            logging.warning(f"WebSocket connection closed for {binance_symbol}. Reconnecting...")
        except Exception as e:
            logging.error(f"Error in Binance WebSocket for {binance_symbol}: {e}")
        finally:
            logging.info(f"Reconnecting to Binance WebSocket for {binance_symbol}...")
            await asyncio.sleep(5)


def process_data(symbol):
    global last_process_time
    current_time = time.time() * 1000
    time_diff = current_time - last_process_time[symbol]
    last_process_time[symbol] = current_time

    # Add scaling factor for 1000CAT to CAT conversion
    scale_factor = 1000 if symbol == 'CAT' else 1

    okx_data_available = latest_data[symbol]['local_orderbook']['time'] is not None
    if latest_data[symbol]['binance']['time'] != 0 and okx_data_available:
        if rate_limiter.should_process(symbol):
            current_time = get_current_time_ms()

            # Use the local orderbook for OKX data
            okx_latest = latest_data[symbol]['local_orderbook']

            # Scale Binance prices by 1000 for CAT
            binance_bids = get_top_n(latest_data[symbol]['binance']['bids'], 5, reverse=True)
            binance_asks = get_top_n(latest_data[symbol]['binance']['asks'], 5)

            if symbol == 'CAT':
                binance_bids = [(price / scale_factor, qty * scale_factor) for price, qty in binance_bids]
                binance_asks = [(price / scale_factor, qty * scale_factor) for price, qty in binance_asks]

            combined_data = {
                'timestamp': get_current_utc_time_with_ms(),
                'binance': {
                    'time': latest_data[symbol]['binance']['time'],
                    'bids': binance_bids,
                    'asks': binance_asks
                },
                'okx': okx_latest,
                'timelag': current_time - min(latest_data[symbol]['binance']['time'], okx_latest['time'])
            }

            impact_bid_okx = okx_latest['bids'][0][0]
            impact_ask_okx = okx_latest['asks'][0][0]
            impact_bid_binance = combined_data['binance']['bids'][0][0]
            impact_ask_binance = combined_data['binance']['asks'][0][0]

            if all(x is not None for x in [impact_bid_okx, impact_ask_okx, impact_bid_binance, impact_ask_binance]):
                combined_data_impact = {
                    'timestamp': get_current_utc_time_with_ms(),
                    'entry_spread': round(100*(impact_bid_okx - impact_ask_binance)/impact_ask_binance, 4),
                    'exit_spread': round(100*(impact_ask_okx - impact_bid_binance)/impact_bid_binance, 4),
                    'best_bid_price_okx': okx_latest['bids'][0][0],
                    'best_ask_price_okx': okx_latest['asks'][0][0],
                    'best_bid_price_binance': combined_data['binance']['bids'][0][0],
                    'best_ask_price_binance': combined_data['binance']['asks'][0][0],
                    'impact_bid_price_okx': round(impact_bid_okx, 7),
                    'impact_ask_price_okx': round(impact_ask_okx, 7),
                    'impact_bid_price_binance': round(impact_bid_binance, 7),
                    'impact_ask_price_binance': round(impact_ask_binance, 7),
                    'okx_orderbook': okx_latest,
                    'binance_orderbook': combined_data['binance'],
                    'timelag': combined_data['timelag'],
                    'impact_price_reached': True
                }
                if impact_bid_okx > impact_ask_okx:
                    logging.info(
                        f'OKX {symbol}"s impact bid {impact_bid_okx} is greater than its impact ask {impact_ask_okx} ')
                if impact_bid_binance > impact_ask_binance:
                    logging.info(
                        f'OKX {symbol}"s impact bid {impact_bid_binance} is greater than its impact ask {impact_ask_binance} ')

            else:
                combined_data_impact = {
                    'timestamp': get_current_utc_time_with_ms(),
                    'entry_spread': None,
                    'exit_spread': None,
                    'best_bid_price_okx': okx_latest['bids'][0][0] if okx_latest['bids'] else None,
                    'best_ask_price_okx': okx_latest['asks'][0][0] if okx_latest['asks'] else None,
                    'best_bid_price_binance': combined_data['binance']['bids'][0][0] if combined_data['binance']['bids'] else None,
                    'best_ask_price_binance': combined_data['binance']['asks'][0][0] if combined_data['binance']['asks'] else None,
                    'impact_bid_price_okx': None,
                    'impact_ask_price_okx': None,
                    'impact_bid_price_binance': None,
                    'impact_ask_price_binance': None,
                    'okx_orderbook': okx_latest,
                    'binance_orderbook': combined_data['binance'],
                    'timelag': combined_data['timelag'],
                    'impact_price_flag': False
                }

            redis_client.rpush(f'combined_data_{symbol}', json.dumps(combined_data_impact))
            redis_client.ltrim(f'combined_data_{symbol}', -500, -1)
            print(f'{time_diff:.2f}ms | {symbol} - {combined_data_impact}')
        else:
            logging.debug(f"Rate limited: Skipping processing for {symbol}")
    else:
        logging.debug(f"Not enough data to process for {symbol}")


async def main():
    tasks = []
    for symbol in symbols:
        okx_uri = "wss://ws.okx.com:8443/ws/v5/public"

        for stream_type in okx_stream_types:
            tasks.append(okx_websocket_handler(okx_uri, symbol, stream_type))

        tasks.append(binance_websocket_handler(symbol))

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
    asyncio.run(run())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Script terminated by user")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        logging.info("Restarting the script...")