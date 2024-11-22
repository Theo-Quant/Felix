import asyncio
import json
import ccxt.async_support as ccxt_async
import redis
import config
from datetime import datetime

# Initialize Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

OKX_CONTRACT_SZ = config.OKX_CONTRACT_SZ

# Initialize Binance and OKX clients
binance = ccxt_async.binance({
    'apiKey': config.BINANCE_API_KEY,
    'secret': config.BINANCE_SECRET_KEY,
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

okx = ccxt_async.okx({
    'apiKey': config.OKX_API_KEY,
    'secret': config.OKX_SECRET_KEY,
    'password': config.OKX_PASSPHRASE,
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

# Configuration
SYMBOL = 'TIA/USDT'
SPREAD_THRESHOLD = 0.04  # 1% spread threshold
QUANTITY = 2  # Example quantity, adjust as needed



async def place_okx_binance_market_orders(symbol, side, quantity):
    perp_symbol = SYMBOL.replace('/USDT', '-USDT-SWAP')
    contract_quantity = quantity / OKX_CONTRACT_SZ.get(perp_symbol)
    print(contract_quantity)
    async def place_okx_order():
        try:
            perp_symbol = SYMBOL.replace('/USDT', '-USDT-SWAP')
            contract_quantity = quantity / OKX_CONTRACT_SZ.get(perp_symbol)
            print(contract_quantity)
            order = await okx.create_market_order(perp_symbol, 'sell', contract_quantity)
            print('OKX Order:', order)
            return {'exchange': 'OKX', 'order_id': order['id']}
        except Exception as e:
            print(f"Failed to place OKX market order: {e}")
            return {'exchange': 'OKX', 'order_id': None, 'error': str(e)}

    async def place_binance_order():
        try:
            order = await binance.create_market_order(symbol, 'buy', quantity)
            print('Binance Order:', order)
            return {'exchange': 'Binance', 'order_id': order['id']}
        except Exception as e:
            print(f"Failed to place Binance market order: {e}")
            return {'exchange': 'Binance', 'order_id': None, 'error': str(e)}

    okx_order, binance_order = await asyncio.gather(
        place_okx_order(),
        place_binance_order()
    )

    return {
        'OKX': okx_order,
        'Binance': binance_order
    }


async def track_spread_and_place_orders():
    while True:
        try:
            # Fetch latest data from Redis
            Clean_Symbol = SYMBOL.replace('/USDT', '')
            latest_data = redis_client.lrange(f'combined_data_{Clean_Symbol}', -1, -1)
            if latest_data:
                latest_row = json.loads(latest_data[0])
                spread = latest_row.get('exit_spread')
                best_bid_price_okx = latest_row.get('best_bid_price_okx')
                best_ask_price_okx = latest_row.get('best_ask_price_okx')
                best_bid_price_binance = latest_row.get('best_bid_price_binance')
                best_ask_price_binance = latest_row.get('best_ask_price_binance')

                print(f"Current spread for {SYMBOL}: {spread} | OKX (B/A) : {best_bid_price_okx}/{best_ask_price_okx} | Binance (B/A) : {best_bid_price_binance}/{best_ask_price_binance}")

                if spread and SPREAD_THRESHOLD < spread:
                    print(f"Spread {spread} exceeds threshold {SPREAD_THRESHOLD}. Placing orders...")
                    result = await place_okx_binance_market_orders(SYMBOL, 'buy', QUANTITY)
                    print(f"Order placement results: {result}")

            await asyncio.sleep(5)  # Check every second
        except Exception as e:
            print(f"Error: {e}")
            await asyncio.sleep(5)  # Wait 5 seconds before retrying on error


async def main():
    await track_spread_and_place_orders()


if __name__ == "__main__":
    asyncio.run(main())