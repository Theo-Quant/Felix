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
from byBitHyperLiquid import rate_limiter, get_current_time_ms, get_current_utc_time_with_ms, get_top_n, calculate_impact_price, process_data, update_local_orderbook, latest_data
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("websocketByHyper.log"),
        logging.StreamHandler()
    ]
)
hyperliquid_ws_url = "wss://api.hyperliquid.xyz/ws"
hyperliquid_stream_types = ['l2Book']
bybit_stream_types = [1, 50, 200, 500] # need to find the stream for this one the depth, use the websocket for this
symbols = ['BTC', 'SOL', 'ETH'] # for hyperliquid
# hyperliquid_message = {
#     "method": "subscribe",
#     "subscription":{ "type": "l2Book", "coin": "BTC" }
# }
orderbook_data = list()
# latest_data = {symbol: {
#     'hyperliquid': {'bids': defaultdict(float), 'asks': defaultdict(float), 'time': 0},
#     'bybit': {
#         stream_type: {'bids': [], 'asks': [], 'time': None}
#         for stream_type in bybit_stream_types
#     },
#     'local_orderbook': {'bids': [], 'asks': [], 'time': None}
# } for symbol in symbols}
last_process_time = {symbol: 0 for symbol in symbols}
async def hyperliquid_websocket_handler(ws_url, symbol, stream_type): # return  [level1, level2] such that levels = [px(price), sz(size), n(number of trades)] , levels1 = bid, levels2 = as
    global latest_data
    while True:
        try:
            async with websockets.connect(ws_url) as websocket:
                logging.info(f'Connected to {ws_url}')
                subscribe_message ={
                    "method": "subscribe",
                    "subscription": {"type": stream_type, "coin": symbol}
                }
                await websocket.send(json.dumps(subscribe_message))
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
    # logging.debug(f"Received hyperliquid message for {symbol} ({stream_type}): {message}")
    # logging.info(f"received message is {message}")
    data = json.loads(message) #parsing the data
    time  = 0
    if 'data' in data:
        if 'levels' in data["data"]:
            levels = data["data"]["levels"]
            bids = levels[0] # [{'px': '97403', 'sz':'4.6913', 'n':'10'}]
            asks = levels[1] #[{'px': '97403', 'sz':'4.6913', 'n':'10'}]
            # print("bids:", bids)
            # print("asks:", asks)
            bids = [[float(bid['px']), float(bid['sz'])] for bid in bids]
            asks = [[float(ask['px']), float(ask['sz'])] for ask in asks]
            new_data = {
                'time': data['data']['time'],
                'bids': bids,
                'asks': asks
            }
            latest_data[symbol]['hyperliquid'][stream_type] = new_data
            update_local_orderbook(symbol, stream_type, new_data)
            # print(latest_data)
            process_data(symbol)

    else:
        logging.warning(f"Unexpected message structure for {symbol} ({stream_type}): {data}")

asyncio.run(hyperliquid_websocket_handler(hyperliquid_ws_url, symbols[0], hyperliquid_stream_types[0]))
