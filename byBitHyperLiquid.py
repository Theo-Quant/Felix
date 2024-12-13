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
from pybit.unified_trading import WebSocket


#basic log info files
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("websocketByHyper.log"),
        logging.StreamHandler()
    ]
)
redis_client = redis.Redis(host='localhost', port=6379, db=0)
# bybit_ws_url = "wss://stream.bybit.com/realtime" # maybe do not need it
hyperliquid_ws_url = "wss://api.hyperliquid.xyz/ws"
hyperliquid_stream_types = ['l2Book']
bybit_stream_types = [1, 50, 200, 500] # need to find the stream for this one the depth, use the websocket for this
symbols = ['BTC', 'SOL', 'ETH'] # for hyperliquid
# hyperliquid_message = {
#     "method": "subscribe",
#     "subscription":{ "type": "l2Book", "coin": "BTC" }
# }
orderbook_data = list()
latest_data = {symbol: {
    'hyperliquid': {'bids': defaultdict(float), 'asks': defaultdict(float), 'time': 0},
    'bybit': {
        stream_type: {'bids': {}, 'asks': {}, 'time': None}
        for stream_type in bybit_stream_types
    },
    'local_orderbook': {'bids': [], 'asks': [], 'time': None}
} for symbol in symbols}
last_process_time = {symbol: 0 for symbol in symbols}
#done
def update_local_orderbook(symbol, stream_type, new_data): #confirmed
    global latest_data
    # local_ob = latest_data[symbol]['local_orderbook']
    print("new_data:", new_data)
    def update_side(side, new_levels):
        current_levels = {price: size for price, size in latest_data[symbol]['local_orderbook'][side]}
        for price, size in new_levels:
            if size == 0:
                current_levels.pop(price, None)
            else:
                current_levels[price] = size

        sorted_levels = sorted(current_levels.items(), key=lambda x: -x[0] if side == 'bids' else x[0])
        return sorted_levels[:5]

    if stream_type == 'l2Book':
        # Replace the entire local orderbook with books5 data
        latest_data[symbol]['local_orderbook']['bids'] = new_data['bids']
        latest_data[symbol]['local_orderbook']['asks'] = new_data['asks']
    else:
        # Update the local orderbook with new data
        latest_data[symbol]['local_orderbook']['bids'] = update_side('bids', new_data['bids'])
        latest_data[symbol]['local_orderbook']['asks'] = update_side('asks', new_data['asks'])

    # Ensure we always have 5 levels for both bids and asks
    while len(latest_data[symbol]['local_orderbook']['bids']) < 5:
        latest_data[symbol]['local_orderbook']['bids'].append((0, 0))
    while len(latest_data[symbol]['local_orderbook']['asks']) < 5:
        latest_data[symbol]['local_orderbook']['asks'].append((float('inf'), 0))

    latest_data[symbol]['local_orderbook']['time'] = new_data['time']
def get_timestamp(): #confirmed
    return int(time.time())
def sign(message, secret_key): #confirmed
    mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    return base64.b64encode(d)

class RateLimiter: #confirmed
    def __init__(self, interval):
        self.interval = interval
        self.last_check = {}

    def should_process(self, symbol):
        current_time = time.time()
        if symbol not in self.last_check or current_time - self.last_check[symbol] >= self.interval:
            self.last_check[symbol] = current_time
            return True
        return False
def handle_message(message):
    logging.info(f"Received message: {message}")
def get_current_utc_time_with_ms(): #confirmed
    now = datetime.now(timezone.utc)
    return now.isoformat(timespec='milliseconds')
def round_significant_digits(value, significant_digits): #confirmed
    if value == 0:
        return 0
    d = Decimal(value)
    rounded_value = d.scaleb(-d.adjusted()).quantize(Decimal(10) ** -significant_digits, rounding=ROUND_HALF_UP).scaleb(d.adjusted())
    return float(rounded_value)
def get_current_time_ms(): #confirmed
    return int(time.time() * 1000)

rate_limiter = RateLimiter(interval=0.025)
def calculate_impact_price(order_book, imn) ->float: #confirmed
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

    # orderbook_data.append(message["data"])
    #TODO1
async def hyperliquid_websocket_handler(ws_url, symbol, stream_type): # return  [level1, level2] such that levels = [px(price), sz(size), n(number of trades)] , levels1 = bid, levels2 = ask
    global latest_data
    while True:
        try:
            async with websockets.connect(ws_url) as websocket:
                print("connected to ")
                logging.info(f'Connected to {ws_url}')
                subscribe_message ={
                    "method": "subscribe",
                    "subscription": {"type": stream_type, "coin": symbol}
                }
                await websocket.send(json.dumps(subscribe_message))
                # message = await websocket.recv()
                # process_hyperliquid_message(symbol, stream_type, message)
                async for message in websocket:
                    try:
                        process_hyperliquid_message(symbol, stream_type, message)
                    except Exception as e:
                        logging.error(f"Error processing message for {symbol} ({stream_type}): {e}")
                        logging.debug(f"Problematic message: {message}")
        except websockets.exceptions.ConnectionClosed:
                logging.warning(f"Hyperliquid WebSocket connection closed for {symbol} ({stream_type}). Reconnecting...")
        except Exception as e:
            logging.error(f"Error in Hyperliquid WebSocket for {symbol} ({stream_type}): {e}")
        finally:
            logging.info(f"Reconnecting to Hyperliquid WebSocket for {symbol} ({stream_type})...")
            await asyncio.sleep(5)
#TODO2
def process_hyperliquid_message(symbol, stream_type, message):
    global latest_data
    logging.debug(f"Received hyperliquid message for {symbol} ({stream_type}): {message}")
    logging.info(f"received message is {message}")
    data = json.loads(message)  # parsing the data
    print(f"Process Hyperliquid message received message is {message}")
    time = 0
    if 'data' in data:
        if 'levels' in data["data"]:
            levels = data["data"]["levels"]
            bids = levels[0]  # [{'px': '97403', 'sz':'4.6913', 'n':'10'}]
            asks = levels[1]  # [{'px': '97403', 'sz':'4.6913', 'n':'10'}]
            # print("bids:", bids)
            # print("asks:", asks)
            bids = [[float(bid['px']), float(bid['sz'])] for bid in bids]
            asks = [[float(ask['px']), float(ask['sz'])] for ask in asks]
            new_data = {
                'time': data['data']['time'],
                'bids': bids,
                'asks': asks
            }
            # print(new_data)
            latest_data[symbol]['hyperliquid'][stream_type] = new_data
            print("new_data:" , new_data)
            update_local_orderbook(symbol, stream_type, new_data)
            process_data(symbol)

    else:
        logging.warning(f"Unexpected message structure for {symbol} ({stream_type}): {data}")
def get_top_n(order_dict, n=5, reverse=False): #confirmed , only value if the dictionary is in a form of dictionary with a format list of  [[price1:quantity1],[price2:quantity2], .. , [pricen:quantityn]] and in
    #works for the bybit
    sorted_orders = sorted(order_dict.items(), key=lambda x: float(x[0]), reverse=True)
    return [(float(price), float(quantity)) for price, quantity in sorted_orders[:n]]
def cleanup_orderbook(symbol): #confirmed
    current_time = time.time()
    cutoff_time = current_time - 300  # Remove entries older than 5 minutes

    for book_type in ['bids', 'asks']:
        latest_data[symbol]['bybit'][book_type] = {
            price: qty for price, qty in latest_data[symbol]['bybit'][book_type].items()
                if latest_data[symbol]['binance']['time'] > cutoff_time
        }
#TODO3
def process_bybit_message(message, symbol, stream_type): # returns {"s': symbol , "ts": timestamp(ms), "b": list of bids in  a form of [bid price, bid size], "a": list of bids in  a form of [ask price, ask size], "u": updateID}
    # logging.debug(f"Received Binance message for {symbol} and {stream_type}")
    event_time= message['ts'] #
    if 'data' in message:
             # returns {"s": symbol, "b": list of bids in  a form of [bid price, bid size], "a": list of bids in  a form of [ask price, ask size]}
        # print(message['data'])
        bid = message['data']['b'] #[[bid price1, bid_size1], [bid_price2, bid_size2], .. , [bid_priceN, bid_sizeN]]
        ask = message['data']['a']#[[ask_price1, ask_size1], [ask_price2, ask_size2], .. , [ask_priceN, ask_sizeN]]
        bid = {float(price) : float(size) for price, size in bid}
        ask = {float(price) : float(size) for price, size in ask}
        latest_data[symbol]['bybit'][stream_type]['bids'] = bid
        latest_data[symbol]['bybit'][stream_type]['asks'] = ask
        latest_data[symbol]['bybit'][stream_type]['time'] = event_time
        # print(latest_data[symbol]['bybit'][stream_type])
        process_data(symbol, bybit_stream=stream_type)
#TODO4
async def bybit_websocket_handler(symbol, depth) : #returns dict('b':[bid price, bid size], 'a':[ask_price, ask_size])
    while True:
        ws = WebSocket(
            testnet=False,
            channel_type="linear",
        )
        ws.orderbook_stream(
            depth=depth,
            symbol=f'{symbol}USDT',
            callback=lambda message: process_bybit_message(message, symbol=symbol, stream_type = depth))
# Run the asyncio event loop
# asyncio.run(hyperliquid_stream())
# asyncio.run(bybit_stream())
#TODO5
def process_data(symbol, bybit_stream = None):
    global last_process_time
    global latest_data
    current_time = time.time() * 1000
    time_diff = current_time - last_process_time[symbol]
    last_process_time[symbol] = current_time
    hyperliquid_data_available = latest_data[symbol]['local_orderbook']['time']  is not None
    bybit_time =  latest_data[symbol]['bybit'][bybit_stream]['time']
    print("before processing")
    print(f" latest data {latest_data}")
    if hyperliquid_data_available and latest_data[symbol]['bybit'][bybit_stream]['time'] is not None :
        if rate_limiter.should_process(symbol):
            current_time = get_current_time_ms()
            #use the local orderbook for bybit data
            hyperliquid_latest = latest_data[symbol]['local_orderbook']
            combined_data = {
                'timestamp': get_current_utc_time_with_ms(),
                'bybit': {
                    'time': latest_data[symbol]['bybit'][bybit_stream]['time'],
                    'bids': get_top_n(latest_data[symbol]['bybit'][bybit_stream]['bids'], 5, reverse=True),
                    'asks': get_top_n(latest_data[symbol]['bybit'][bybit_stream]['asks'], 5)
                },
                'hyperliquid': hyperliquid_latest,
                'timelag': current_time - min(latest_data[symbol]['bybit'][bybit_stream]['time'], hyperliquid_latest['time'])
            }
            impact_bid_hyperliquid = calculate_impact_price(hyperliquid_latest['bids'], 100)
            impact_ask_hyperliquid = calculate_impact_price(hyperliquid_latest['asks'], 100)
            impact_bid_bybit = calculate_impact_price(combined_data['bybit']['bids'], 100) #confirmed
            impact_ask_bybit = calculate_impact_price(combined_data['bybit']['asks'], 100) #confirmed
            if all(x is not None for x in [impact_bid_hyperliquid, impact_ask_hyperliquid, impact_bid_bybit, impact_ask_bybit]): #means all of the component in iterator should not be none
                combined_data_impact = {
                    'timestamp': get_current_utc_time_with_ms(),
                    'best_bid_price_hyperliquid': hyperliquid_latest['bids'][0][0],
                    'best_ask_price_hyperliquid': hyperliquid_latest['asks'][0][0],
                    'best_bid_price_bybit': combined_data['bybit']['bids'][0][0],
                    'best_ask_price_bybit': combined_data['bybit']['asks'][0][0],
                    'entry_spread': round(100 * (hyperliquid_latest['bids'][0][0] - combined_data['bybit']['asks'][0][0]) / combined_data['bybit']['asks'][0][0], 4),
                    'exit_spread': round(100 * (hyperliquid_latest['asks'][0][0] - combined_data['bybit']['bids'][0][0]) / combined_data['bybit']['bids'][0][0], 4),
                    'hyperliquid_orderbook': hyperliquid_latest,
                    'bybit_orderbook': combined_data['bybit'],
                    'timelag': combined_data['timelag'],
                    'impact_price_reached': True
                }
                if impact_bid_hyperliquid > impact_ask_hyperliquid:
                    logging.info(
                        f'Hyperliquid {symbol}"s impact bid {impact_bid_hyperliquid} is greater than its impact ask {impact_ask_hyperliquid} ')
                if impact_bid_bybit > impact_ask_bybit:
                    logging.info(
                        f'Hyperliquid {symbol}"s impact bid {impact_bid_bybit} is greater than its impact ask {impact_ask_bybit} ')
            else:
                combined_data_impact = {
                    'timestamp': get_current_utc_time_with_ms(),
                    'entry_spread': None,
                    'exit_spread': None,
                    'best_bid_price_hyperliquid': hyperliquid_latest['bids'][0][0] if hyperliquid_latest['bids'] else None,
                    'best_ask_price_hyperliquid': hyperliquid_latest['asks'][0][0] if hyperliquid_latest['asks'] else None,
                    'best_bid_price_bybit': combined_data['bybit']['bids'][0][0] if combined_data['bybit']['bids'] else None,
                    'best_ask_price_bybit': combined_data['bybit']['asks'][0][0] if combined_data['bybit']['asks'] else None,
                    'impact_bid_price_hyperliquid': None,
                    'impact_ask_price_hyperliquid': None,
                    'impact_bid_price_hyperliquid': None,
                    'impact_ask_price_hyperliquid': None,
                    'hyperliquid_orderbook': hyperliquid_latest,
                    'bybit_orderbook': combined_data['bybit'],
                    'timelag': combined_data['timelag'],
                    'impact_price_flag': False
                }
            # redis_client.rpush(f'combined_data_{symbol}', json.dumps(combined_data_impact))
            # redis_client.ltrim(f'combined_data_{symbol}', -500, -1)
            print(f'{time_diff:.2f}ms | {symbol} - {combined_data_impact}')
        else:
            logging.debug(f"Rate limited: Skipping processing for {symbol}")
    else:
        logging.debug(f"Not enough data to process for {symbol}")


async def main():
    tasks = []
    for symbol in symbols:
        for stream_type in hyperliquid_stream_types:
            tasks.append(hyperliquid_websocket_handler(hyperliquid_ws_url , symbol, stream_type))
        for stream_type in bybit_stream_types:
            tasks.append(bybit_websocket_handler(symbol,50))
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
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Script terminated by user")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        logging.info("Restarting the script...")