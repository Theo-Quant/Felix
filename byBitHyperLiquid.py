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
bybit_ws_url = "wss://stream.bybit.com/realtime"
hyperliquid_ws_url = "wss://api.hyperliquid.xyz/ws"
hyperliquid_stream_types = ['l2Book']
bybit_stream_types = [1, 50, 200, 500] # need to find the stream for this one the depth, use the websocket for this
symbol = ['BTC', 'SOL']
hyper_message = {
    "method": "subscribe",
    "subscription":{ "type": "l2Book", "coin": "BTC" }
}
def handle_message(message):
    logging.info(f"Received message: {message}")
async def hyperliquid_stream(): # return  px(price), sz(size), n(number of trades)
    try:
        # Connect to the WebSocket server
        async with websockets.connect(hyperliquid_ws_url) as websocket:
            logging.info("Connected to Hyperliquid WebSocket")

            # Send subscription message
            await websocket.send(json.dumps(hyper_message))
            logging.info("Sent subscription message: %s", json.dumps(hyper_message))

            # Receive data from WebSocket
            while True:
                data = await websocket.recv()
                # logging.info("Received data: %s", data)
                handle_message(data)


    except Exception as e:
        logging.error("Error: %s", str(e))
async def bybit_stream(): #returns bid price, bid size
    ws = WebSocket(
        testnet=False,
        channel_type="linear",
    )
    ws.orderbook_stream(
        depth=50,
        symbol="BTCUSDT",
        callback=handle_message)
# Run the asyncio event loop
asyncio.run(bybit_stream())