"""
Script to subscribe to OKX Pre Market Futures.
"""


import asyncio
import websockets
import json
import logging
from datetime import datetime
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)


class MultiCoinOrderbookMonitor:
    def __init__(self, coins):
        self.coins = coins
        self.okx_url = "wss://ws.okx.com:8443/ws/v5/public"
        # Initialize orderbook data for each coin
        self.latest_data = {
            coin: {
                'okx': {'bids': [], 'asks': [], 'time': None},
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
                            {"channel": "books5", "instId": f"{coin}-USDT-250606"} # This is the instID purely for Hyper expiry future
                            for coin in self.coins
                        ]
                    }
                    await ws.send(json.dumps(subscribe_msg))

                    async for msg in ws:
                        data = json.loads(msg)
                        print(data)
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

    async def run(self):
        await asyncio.gather(
            self.connect_okx(),
        )


async def main():
    # List of coins to monitor
    coins = ["HYPE"]

    monitor = MultiCoinOrderbookMonitor(coins)
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())