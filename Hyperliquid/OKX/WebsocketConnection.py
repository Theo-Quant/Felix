"""
Script to subscribe to Hyperliquid Spot and OKX Pre Market Futures.
Need to change red lines in code before using this websocket for other coins.
"""


import asyncio
import websockets
import json
import logging
from datetime import datetime
import time
import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)


class MultiCoinOrderbookMonitor:
    def __init__(self, coins):
        self.coins = coins
        self.okx_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.hyperliquid_url = "wss://api.hyperliquid.xyz/ws"
        # Initialize orderbook data for each coin
        self.latest_data = {
            coin: {
                'okx': {'bids': [], 'asks': [], 'time': None},
                'hyperliquid': {'bids': [], 'asks': [], 'time': None}
            }
            for coin in coins
        }

    async def connect_okx(self):
        while True:
            try:
                async with websockets.connect(self.okx_url) as ws:
                    logging.info("Connected to OKX WebSocket")

                    # Subscribe to all coins
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": [
                            {"channel": "books5", "instId": f"{coin}-USDT-250606"} # Expiry purely for HYPE
                            # This is the instID purely for Hyper expiry future
                            for coin in self.coins
                        ]
                    }
                    await ws.send(json.dumps(subscribe_msg))

                    async for msg in ws:
                        data = json.loads(msg)
                        if 'event' in data:
                            continue

                        if 'data' in data:
                            book_data = data['data'][0]
                            # Extract coin from instId (e.g., "BTC-USDT-SWAP" -> "BTC")
                            inst_id = book_data.get('instId') or data['arg']['instId']
                            coin = inst_id.split('-')[0]

                            if coin in self.coins:
                                self.latest_data[coin]['okx'] = {
                                    'time': int(book_data['ts']),
                                    'bids': [(float(bid[0]), float(bid[1])) for bid in book_data.get('bids', [])],
                                    'asks': [(float(ask[0]), float(ask[1])) for ask in book_data.get('asks', [])]
                                }

            except Exception as e:
                logging.error(f"OKX WebSocket error: {e}")
                await asyncio.sleep(1)

    async def connect_hyperliquid(self):
        while True:
            try:
                async with websockets.connect(self.hyperliquid_url) as ws:
                    logging.info("Connected to Hyperliquid WebSocket")

                    # Subscribe to all coins
                    for coin in self.coins:
                        subscribe_msg = {
                            "method": "subscribe",
                            "subscription": {
                                "type": "l2Book",
                                "coin": '@107' # Coin name for HYPE
                            }
                        }
                        await ws.send(json.dumps(subscribe_msg))

                    async for msg in ws:
                        data = json.loads(msg)

                        if 'channel' in data and data['channel'] == 'l2Book' and 'data' in data:
                            book_data = data['data']
                            # coin = book_data.get('coin')
                            coin = 'HYPE'

                            if coin in self.coins and 'levels' in book_data:
                                self.latest_data[coin]['hyperliquid'] = {
                                    'time': int(book_data.get('time', time.time() * 1000)),
                                    'bids': [(float(bid['px']), float(bid['sz'])) for bid in book_data['levels'][0]],
                                    'asks': [(float(ask['px']), float(ask['sz'])) for ask in book_data['levels'][1]]
                                }
                                await self.process_orderbooks(coin)

            except Exception as e:
                logging.error(f"Hyperliquid WebSocket error: {e}")
                await asyncio.sleep(1)

    async def process_orderbooks(self, coin):
        """Process and compare orderbooks for a specific coin"""
        okx_data = self.latest_data[coin]['okx']
        hl_data = self.latest_data[coin]['hyperliquid']

        # Only process if we have data from both exchanges
        if okx_data['time'] and hl_data['time']:
            current_time = datetime.now().isoformat()

            # Extract best bid/ask from each exchange
            okx_best_bid = okx_data['bids'][0][0] if okx_data['bids'] else None
            okx_best_ask = okx_data['asks'][0][0] if okx_data['asks'] else None
            hl_best_bid = hl_data['bids'][0][0] if hl_data['bids'] else None
            hl_best_ask = hl_data['asks'][0][0] if hl_data['asks'] else None

            if all(x is not None for x in [okx_best_bid, okx_best_ask, hl_best_bid, hl_best_ask]):
                # Calculate spreads
                entry_spread = round(100 * (okx_best_bid - hl_best_ask) / hl_best_ask, 4)
                exit_spread = round(100 * (okx_best_ask - hl_best_bid) / hl_best_bid, 4)

                # Calculate timelag
                timelag = int(time.time() * 1000) - min(okx_data['time'], hl_data['time'])

                combined_data = {
                    'timestamp': datetime.now().isoformat(timespec='milliseconds'),
                    'entry_spread': entry_spread,
                    'exit_spread': exit_spread,
                    'best_bid_price_okx': okx_best_bid,
                    'best_ask_price_okx': okx_best_ask,
                    'best_bid_price_hyperliquid': hl_best_bid,
                    'best_ask_price_hyperliquid': hl_best_ask,
                    'okx_orderbook': okx_data,
                    'hyperliquid_orderbook': hl_data,
                    'timelag': timelag,
                }

                redis_client.rpush(f'HyperliquidOKX_combined_data_{coin}', json.dumps(combined_data))
                redis_client.ltrim(f'HyperliquidOKX_combined_data_{coin}', -500, -1)

                print(f'{coin} - {combined_data}')

    async def run(self):
        await asyncio.gather(
            self.connect_okx(),
            self.connect_hyperliquid()
        )


async def main():
    # List of coins to monitor
    coins = ["HYPE"]
    monitor = MultiCoinOrderbookMonitor(coins)
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())