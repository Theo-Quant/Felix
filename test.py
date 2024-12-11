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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("websocketByHyper.log"),
        logging.StreamHandler()
    ]
)
symbols = ["BTC", "SOL", "ETH"]
hyperliquid_ws_url = "wss://api.hyperliquid.xyz/ws"
hyperliquid_stream_types = ["l2Book"]

async def hyperliquid_websocket_handler(ws_url, symbol, stream_type): # return  [level1, level2] such that levels = [px(price), sz(size), n(number of trades)] , levels1 = bid, levels2 = as
    while True:
        try:
            async with websockets.connect(ws_url) as websocket:
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
    logging.debug(f"Received hyperliquid message for {symbol} ({stream_type}): {message}")
    logging.info(f"received message is {message}")
    data = json.loads(message)


asyncio.run(hyperliquid_websocket_handler(hyperliquid_ws_url, symbols[0], hyperliquid_stream_types[0]))
