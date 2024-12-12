# ByBit&HyperLiquid WebSocket Integration

## Libraries 
```python
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
```
## Introduction

This project, `byBitHyperLiquid.py`, establishes WebSocket connections to both the Bybit and HyperLiquid trading platforms

## Compilation Command
* Compiled by writing in terminal `python byBitHyperLiquid.py`.

## Documentation 
* For Bybit documentation, refer to  https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook  for the websocket API. Furthermore to understand `from pybit.unified_trading import WebSocket`, refer to https://github.com/bybit-exchange/pybit/blob/master/pybit/unified_trading.py method `def orderbook_stream` 
* For Hyperliquid Documentation, refer to  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket for the websocket API.

## Variables
The symbols in Hyperliquid is showing the pattern such as ETH, BTC, DOGE and etc whilst the symbols in Bybit shows the pattern of 
BTCUSDT, ETHUSDT, DOGEUSDT which is given in Hyperliquid`symbols = ['ETH', 'BTC', 'DOGE', 'SOL']` and  for bybit `[f'{symbol}USDT' for symbol in sybmols]`.

## Methods
* Use  `json.dump()` method that can convert a Python object into a JSON string. Can be used for writing/dumping JSON to a file/socket.
* Use `json.load()` method to parse the JSON object to the python dictionary object
* For Bybit, use the `from pybit.unified_trading import WebSocket` to use the WebSocket package which WebSocket returns dict type. Don't have to parse JSON object.