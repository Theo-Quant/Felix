import aiohttp
import asyncio
import json
from datetime import datetime
import config

# Use your Binance API key and secret here
API_KEY = config.BINANCE_API_KEY
API_SECRET = config.BINANCE_SECRET_KEY

# Choose the appropriate base URL
SPOT_BASE_URL = 'https://api.binance.com'
FUTURES_BASE_URL = 'https://fapi.binance.com'

# Set this to True for futures, False for spot
USE_FUTURES = True

BASE_URL = FUTURES_BASE_URL if USE_FUTURES else SPOT_BASE_URL
ENDPOINT = '/fapi/v1/exchangeInfo' if USE_FUTURES else '/api/v3/exchangeInfo'


async def get_exchange_info():
    url = f"{BASE_URL}{ENDPOINT}"

    headers = {
        'X-MBX-APIKEY': API_KEY
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                print(f"Error: {response.status}")
                return None


def parse_rate_limits(exchange_info):
    rate_limits = exchange_info.get('rateLimits', [])

    for limit in rate_limits:
        limit_type = limit['rateLimitType']
        interval = limit['interval']
        interval_num = limit['intervalNum']
        limit_value = limit['limit']

        print(f"Type: {limit_type}")
        print(f"Interval: {interval_num} {interval}")
        print(f"Limit: {limit_value}")
        print("---")


async def main():
    exchange_info = await get_exchange_info()

    if exchange_info:
        print("Rate Limits:")
        parse_rate_limits(exchange_info)
        print(exchange_info)

        # Check used weight
        headers = exchange_info.get('headers', {})
        used_weight = headers.get('X-MBX-USED-WEIGHT-1M', 'N/A')
        print(f"Used weight in the last 1 minute: {used_weight}")


if __name__ == "__main__":
    asyncio.run(main())