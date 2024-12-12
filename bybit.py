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
from time import sleep
from byBitHyperLiquid import update_local_orderbook, process_data, rate_limiter
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
latest_data = {symbol: {
    'hyperliquid': {'bids': defaultdict(float), 'asks': defaultdict(float), 'time': 0},
    'bybit': {
        stream_type: {'bids': {}, 'asks': {}, 'time': None}
        for stream_type in bybit_stream_types
    },
    'local_orderbook': {'bids': [], 'asks': [], 'time': None}
} for symbol in symbols}
last_process_time = {symbol: 0 for symbol in symbols}
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
        print(latest_data[symbol]['bybit'][stream_type])
        # process_data(symbol)
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
            callback=lambda message: process_bybit_message(message, symbol=symbol, stream_type = depth)) #already returns a dictionary type in process_bybit_message
asyncio.run(bybit_websocket_handler(symbols[0], 50))












