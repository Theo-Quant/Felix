import ccxt.async_support as ccxt
import asyncio
import json
from pprint import pprint


async def fetch_hyperliquid_markets():
    # Initialize Hyperliquid exchange
    hyperliquid = ccxt.hyperliquid({
        'enableRateLimit': True
    })

    try:
        # Load markets
        await hyperliquid.load_markets()

        # Get all markets
        markets = hyperliquid.markets
        print(markets)

        # Create a clean mapping of symbol to intId
        market_ids = {}
        for symbol, market_data in markets.items():
            if 'info' in market_data and 'intId' in market_data['info']:
                market_ids[symbol] = market_data['info']['intId']

        # Sort by intId for cleaner output
        sorted_markets = dict(sorted(market_ids.items(), key=lambda x: int(x[1])))

        print("Hyperliquid Market IDs:")
        print("=======================")
        print(json.dumps(sorted_markets, indent=2))

        # Also print as Python dict format for easy copying
        print("\nAs Python dictionary:")
        print("=====================")
        pprint(sorted_markets)

        return sorted_markets

    except Exception as e:
        print(f"Error fetching markets: {e}")
        return None

    finally:
        await hyperliquid.close()


async def main():
    await fetch_hyperliquid_markets()


if __name__ == "__main__":
    asyncio.run(main())