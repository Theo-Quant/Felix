import ccxt.async_support as ccxt
import pandas as pd
import sqlite3
import asyncio
from datetime import datetime
import config
from TimeOffset import TimeSync  # Import the TimeSync class

# Configuration
COINS = config.COINS
SPREAD_THRESHOLD = 0.5  # Percentage threshold for a good spread
DATABASE = r'C:\Users\Lewis Lu\PycharmProjects\Hyperliquid\CrossExchangeMarketMaking\LiveTestingV2\FundingRateArbitrage\arbitrage_data.db'
logger = config.setup_logger('SpreadTracker')


# Initialize exchanges
okx = ccxt.okx({
    'apiKey': config.OKX_API_KEY,
    'secret': config.OKX_SECRET_KEY,
    'password': config.OKX_PASSPHRASE,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',
    }
})
binance = ccxt.binance({
    'apiKey': config.BINANCE_API_KEY,
    'secret': config.BINANCE_SECRET_KEY,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future'
    }
})

# Initialize TimeSync
time_sync = TimeSync(interval=300)

# Convert symbol format from Binance to OKX
def convert_symbol_to_okx(symbol):
    base, quote = symbol.split('/')
    if quote == 'USDC':
        return f"{base}-USDC-SWAP"
    return f"{base}-{quote}-SWAP"


# Function to fetch prices
async def fetch_prices(exchange, symbol, time_sync):
    try:
        # Adjust the timestamp for the request
        timestamp = time_sync.get_adjusted_time()
        if 'binance' in exchange.id:
            exchange.options['timestamp'] = timestamp
            exchange.options['recvWindow'] = 60000

        ticker = await exchange.fetch_ticker(symbol)
        bid = ticker.get('bid')
        ask = ticker.get('ask')
        last = ticker.get('last')
        timestamp_str = datetime.fromtimestamp(time_sync.get_adjusted_time() / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

        if bid is None or ask is None:
            # Fetch order book as a fallback
            order_book = await exchange.fetch_order_book(symbol)
            bid = order_book['bids'][0][0] if len(order_book['bids']) > 0 else None
            ask = order_book['asks'][0][0] if len(order_book['asks']) > 0 else None

        return bid, ask, last, timestamp_str
    except Exception as e:
        print(f"Error fetching prices for {symbol} on {exchange.id}: {e}")
        return None, None, None, None


# Function to initialize the database
def initialize_database():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS spreads (
            timestamp TEXT,
            coin TEXT,
            binance_bid REAL,
            binance_ask REAL,
            binance_last REAL,
            okx_bid REAL,
            okx_ask REAL,
            okx_last REAL,
            spread_binance_to_okx REAL,
            spread_okx_to_binance REAL
        )
    ''')
    conn.commit()
    conn.close()


# Function to store data in the database
def store_data(df):
    conn = sqlite3.connect(DATABASE)
    df.to_sql('spreads', conn, if_exists='append', index=False)
    print(f'Stored to database at {datetime.now()}')
    conn.close()


async def fetch_data_for_coin(coin):
    okx_symbol = convert_symbol_to_okx(coin)
    binance_data, okx_data = await asyncio.gather(
        fetch_prices(binance, coin, time_sync),
        fetch_prices(okx, okx_symbol, time_sync)
    )

    binance_bid, binance_ask, binance_last, binance_timestamp = binance_data
    okx_bid, okx_ask, okx_last, okx_timestamp = okx_data

    if None in [binance_bid, binance_ask, binance_last, okx_bid, okx_ask, okx_last]:
        print(f"Error: Missing price data for {coin}")
        return None

    spread_binance_to_okx = (okx_bid - binance_ask) / binance_ask * 100
    spread_okx_to_binance = (binance_bid - okx_ask) / okx_ask * 100

    print(f"{coin}: Binance Timestamp: {binance_timestamp}, OKX Timestamp: {okx_timestamp}")

    return (
        pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        coin,
        binance_bid,
        binance_ask,
        binance_last,
        okx_bid,
        okx_ask,
        okx_last,
        spread_binance_to_okx,
        spread_okx_to_binance
    )


# Function to track prices and spreads for all coins
async def track_prices_and_spreads():
    while True:
        try:
            data = []
            for coin in COINS:
                result = await fetch_data_for_coin(coin)
                if result:
                    data.append(result)
                    timestamp, coin, binance_bid, binance_ask, binance_last, okx_bid, okx_ask, okx_last, spread_binance_to_okx, spread_okx_to_binance = result
                    print(f"{coin}: Binance Bid: {binance_bid}, Ask: {binance_ask}, OKX Bid: {okx_bid}, Ask: {okx_ask}")
                    print(
                        f"Spread Binance to OKX: {spread_binance_to_okx:.2f}%, Spread OKX to Binance: {spread_okx_to_binance:.2f}%")

            df = pd.DataFrame(data, columns=[
                'timestamp', 'coin', 'binance_bid', 'binance_ask', 'binance_last',
                'okx_bid', 'okx_ask', 'okx_last', 'spread_binance_to_okx', 'spread_okx_to_binance'
            ])
            print(df)
            store_data(df)
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Error encountered: {e}. Retrying in 30 seconds...")
            await asyncio.sleep(30)


def main():
    try:
        # Initialize the database
        initialize_database()

        # Run the tracking script
        asyncio.run(track_prices_and_spreads())
    except KeyboardInterrupt:
        print("SpreadTracker shutting down...")


if __name__ == "__main__":
    main()
