import sys
import pyodbc
import asyncio
from datetime import datetime
from TimeOffset import TimeSync
import ccxt.async_support as ccxt
import SpreadTrackerConfig as config

# Configuration
PERP_COINS = config.PERP_COINS
SPOT_COINS = config.SPOT_COINS
logger = config.setup_logger('ExchangeDataTracker')
AZURE_SQL_CONNECTION_STRING = config.AZURE_SQL_CONNECTION_STRING

# Initialize exchanges without API keys
binance_spot = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'spot'}})
binance_perp = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
okx_spot = ccxt.okx({'enableRateLimit': True, 'options': {'defaultType': 'spot'}})
okx_perp = ccxt.okx({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
gate_spot = ccxt.gateio({'enableRateLimit': True, 'options': {'defaultType': 'spot'}})
gate_perp = ccxt.gateio({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
bitget_spot = ccxt.bitget({'enableRateLimit': True, 'options': {'defaultType': 'spot'}})
bitget_perp = ccxt.bitget({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
bybit_spot = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'spot'}})
bybit_perp = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
hyperliquid_perp = ccxt.hyperliquid({'enableRateLimit': True})

# Initialize TimeSync
time_sync = TimeSync(interval=300)


def convert_symbol(symbol, exchange):
    """
    Convert a standard trading symbol to the format required by a specific exchange.

    :param symbol: Standard trading symbol (e.g., 'BTC/USDT')
    :param exchange: Name of the exchange ('okx', 'gate', or other)
    :return: Converted symbol in the format required by the specified exchange
    """
    # If you try to modify this function, be aware of some exchanges will return spot data when the perp data is unavailable, and vice versa.
    # Advice is to make sure that the symbol that you are using is indeed fetching the correct data. The defaultType don't matter much if you specify the symbol correctly.
    base, quote = symbol.split('/')
    if isinstance(exchange, ccxt.okx) and exchange.options['defaultType'] == 'spot':
        return f"{base}-{quote}"
    elif isinstance(exchange, ccxt.okx) and exchange.options['defaultType'] == 'swap':
        return f"{base}-{quote}-SWAP"
    elif isinstance(exchange, ccxt.gateio) and exchange.options['defaultType'] == 'spot':
        return f"{base}_{quote}"
    elif isinstance(exchange, ccxt.gateio) and exchange.options['defaultType'] == 'swap':
        return f"{symbol}:{quote}"
    elif isinstance(exchange, ccxt.bitget) and exchange.options['defaultType'] == 'spot':
        return f"{base}{quote}"
    elif isinstance(exchange, ccxt.bitget) and exchange.options['defaultType'] == 'swap':
        return f"{symbol}:{quote}"
    elif isinstance(exchange, ccxt.binance) and exchange.options['defaultType'] == 'future':
        return f"{symbol}:{quote}"
    elif isinstance(exchange, ccxt.binance) and exchange.options['defaultType'] == 'spot':
        return symbol
    elif isinstance(exchange, ccxt.bybit) and exchange.options['defaultType'] == 'spot':
        return f"{base}/{quote}"
    elif isinstance(exchange, ccxt.bybit) and exchange.options['defaultType'] == 'swap':
        return f"{base}/{quote}:{quote}"
    elif isinstance(exchange, ccxt.hyperliquid) and exchange.options['defaultType'] == 'swap':
        return f"{base}/USDC:USDC"
    logger.error(f"Error: Invalid exchange specified for symbol conversion: {exchange.id}")
    return symbol


async def fetch_prices_and_orderbook(exchange, symbol, time_sync):
    """
    Fetch current prices and orderbook data for a given symbol from a specified exchange.

    :param exchange: ccxt exchange object
    :param symbol: Trading symbol
    :param time_sync: TimeSync object for timestamp synchronization
    :return: Dictionary containing bid, ask, last price, timestamp, and orderbook data
    """
    try:
        timestamp = time_sync.get_adjusted_time()
        if isinstance(exchange, ccxt.binance):
            exchange.options['timestamp'] = timestamp
            exchange.options['recvWindow'] = 60000

        # Add debug logging
        logger.info(f"Attempting to fetch orderbook for {symbol} on {exchange.id}")
        orderbook = await exchange.fetch_order_book(symbol, limit=5)
        logger.info(f"Successfully fetched orderbook for {symbol} on {exchange.id}")

        logger.info(f"Attempting to fetch ticker for {symbol} on {exchange.id}")
        ticker = await exchange.fetch_ticker(symbol)
        logger.info(f"Successfully fetched ticker for {symbol} on {exchange.id}")

        timestamp_str = datetime.fromtimestamp(time_sync.get_adjusted_time() / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

        result = {
            'bid': ticker['bid'],
            'ask': ticker['ask'],
            'last': ticker['last'],
            'timestamp': timestamp_str,
            'bids': orderbook['bids'][:5],
            'asks': orderbook['asks'][:5]
        }
        logger.info(f"Successful data fetch for {symbol} on {exchange.id}: {result}")
        return result
    except Exception as e:
        print(f"Error fetching data for {symbol} on {exchange.id}: {e}")
        return None


async def fetch_data_for_coin(coin, fetch_spot):
    """
    Fetch price and orderbook data for a specific coin from multiple exchanges.

    :param coin: Trading symbol of the coin
    :param spot: Fetch spot data if True, otherwise fetch perpetual data
    :return: Dictionary containing aggregated data from all exchanges
    """
    if fetch_spot:
        okx_symbol = convert_symbol(coin, okx_spot)
        gate_symbol = convert_symbol(coin, gate_spot)
        bitget_symbol = convert_symbol(coin, bitget_spot)
        bybit_symbol = convert_symbol(coin, bybit_spot)

        logger.info(f"Fetching data for {coin} spot markets...")
        binance_data, okx_data, gate_data, bitget_data, bybit_data = await asyncio.gather(
            fetch_prices_and_orderbook(binance_spot, coin, time_sync),
            fetch_prices_and_orderbook(okx_spot, okx_symbol, time_sync),
            fetch_prices_and_orderbook(gate_spot, gate_symbol, time_sync),
            fetch_prices_and_orderbook(bitget_spot, bitget_symbol, time_sync),
            fetch_prices_and_orderbook(bybit_spot, bybit_symbol, time_sync)
        )
        hyperliquid_data = None
    else:
        okx_symbol = convert_symbol(coin, okx_perp)
        gate_symbol = convert_symbol(coin, gate_perp)
        bitget_symbol = convert_symbol(coin, bitget_perp)
        bybit_symbol = convert_symbol(coin, bybit_perp)
        hyperliquid_symbol = convert_symbol(coin, hyperliquid_perp)

        logger.info(f"Fetching data for {coin} perpetual markets...")
        binance_data, okx_data, gate_data, bitget_data, bybit_data, hyperliquid_data = await asyncio.gather(
            fetch_prices_and_orderbook(binance_perp, coin, time_sync),
            fetch_prices_and_orderbook(okx_perp, okx_symbol, time_sync),
            fetch_prices_and_orderbook(gate_perp, gate_symbol, time_sync),
            fetch_prices_and_orderbook(bitget_perp, bitget_symbol, time_sync),
            fetch_prices_and_orderbook(bybit_perp, bybit_symbol, time_sync),
            fetch_prices_and_orderbook(hyperliquid_perp, hyperliquid_symbol, time_sync)
        )

    if fetch_spot and all(data is None for data in [binance_data, okx_data, gate_data, bitget_data, bybit_data]):
        print(f"Error: No spot data available for {coin} on any exchange")
        return None
    elif not fetch_spot and all(
            data is None for data in [binance_data, okx_data, gate_data, bitget_data, bybit_data, hyperliquid_data]):
        print(f"Error: No perp data available for {coin} on any exchange")
        return None

    result = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'coin': coin,
        'binance_last': binance_data['last'] if binance_data else None,
        'okx_last': okx_data['last'] if okx_data else None,
        'gate_last': gate_data['last'] if gate_data else None,
        'bitget_last': bitget_data['last'] if bitget_data else None,
        'bybit_last': bybit_data['last'] if bybit_data else None,
        'hyperliquid_last': hyperliquid_data['last'] if hyperliquid_data and not fetch_spot else None,
        'binance_orderbook': binance_data if binance_data else None,
        'okx_orderbook': okx_data if okx_data else None,
        'gate_orderbook': gate_data if gate_data else None,
        'bitget_orderbook': bitget_data if bitget_data else None,
        'bybit_orderbook': bybit_data if bybit_data else None,
        'hyperliquid_orderbook': hyperliquid_data if hyperliquid_data and not fetch_spot else None
    }

    if fetch_spot:
        print(f"{coin} spot: "
              f"Binance Last: {result['binance_last']}, "
              f"OKX Last: {result['okx_last']}, "
              f"Gate Last: {result['gate_last']}, "
              f"Bitget Last: {result['bitget_last']}, "
              f"Bybit Last: {result['bybit_last']}")
    else:
        print(f"{coin} perp: "
              f"Binance Last: {result['binance_last']}, "
              f"OKX Last: {result['okx_last']}, "
              f"Gate Last: {result['gate_last']}, "
              f"Bitget Last: {result['bitget_last']}, "
              f"Bybit Last: {result['bybit_last']}, "
              f"Hyperliquid Last: {result['hyperliquid_last']}")

    return result


def upload_to_azure_sql(data, fetch_spot):
    """
    Upload the collected exchange data to Azure SQL Database with correct column order.
    """
    conn = pyodbc.connect(AZURE_SQL_CONNECTION_STRING)
    cursor = conn.cursor()

    table_name = 'exchange_data_spot' if fetch_spot else 'exchange_dataV2'

    # Add Hyperliquid columns for perpetual table only
    hyperliquid_columns = ""
    if not fetch_spot:
        hyperliquid_columns = """,
            hyperliquid_last,
            hyperliquid_bid1, hyperliquid_ask1, hyperliquid_bid_vol1, hyperliquid_ask_vol1,
            hyperliquid_bid2, hyperliquid_ask2, hyperliquid_bid_vol2, hyperliquid_ask_vol2,
            hyperliquid_bid3, hyperliquid_ask3, hyperliquid_bid_vol3, hyperliquid_ask_vol3,
            hyperliquid_bid4, hyperliquid_ask4, hyperliquid_bid_vol4, hyperliquid_ask_vol4,
            hyperliquid_bid5, hyperliquid_ask5, hyperliquid_bid_vol5, hyperliquid_ask_vol5"""

    insert_query = f"""
    INSERT INTO {table_name} (
        timestamp, coin,
        binance_last, okx_last, gate_last, bitget_last, bybit_last,
        binance_bid1, binance_ask1, binance_bid_vol1, binance_ask_vol1,
        binance_bid2, binance_ask2, binance_bid_vol2, binance_ask_vol2,
        binance_bid3, binance_ask3, binance_bid_vol3, binance_ask_vol3,
        binance_bid4, binance_ask4, binance_bid_vol4, binance_ask_vol4,
        binance_bid5, binance_ask5, binance_bid_vol5, binance_ask_vol5,
        okx_bid1, okx_ask1, okx_bid_vol1, okx_ask_vol1,
        okx_bid2, okx_ask2, okx_bid_vol2, okx_ask_vol2,
        okx_bid3, okx_ask3, okx_bid_vol3, okx_ask_vol3,
        okx_bid4, okx_ask4, okx_bid_vol4, okx_ask_vol4,
        okx_bid5, okx_ask5, okx_bid_vol5, okx_ask_vol5,
        gate_bid1, gate_ask1, gate_bid_vol1, gate_ask_vol1,
        gate_bid2, gate_ask2, gate_bid_vol2, gate_ask_vol2,
        gate_bid3, gate_ask3, gate_bid_vol3, gate_ask_vol3,
        gate_bid4, gate_ask4, gate_bid_vol4, gate_ask_vol4,
        gate_bid5, gate_ask5, gate_bid_vol5, gate_ask_vol5,
        bitget_bid1, bitget_ask1, bitget_bid_vol1, bitget_ask_vol1,
        bitget_bid2, bitget_ask2, bitget_bid_vol2, bitget_ask_vol2,
        bitget_bid3, bitget_ask3, bitget_bid_vol3, bitget_ask_vol3,
        bitget_bid4, bitget_ask4, bitget_bid_vol4, bitget_ask_vol4,
        bitget_bid5, bitget_ask5, bitget_bid_vol5, bitget_ask_vol5,
        bybit_bid1, bybit_ask1, bybit_bid_vol1, bybit_ask_vol1,
        bybit_bid2, bybit_ask2, bybit_bid_vol2, bybit_ask_vol2,
        bybit_bid3, bybit_ask3, bybit_bid_vol3, bybit_ask_vol3,
        bybit_bid4, bybit_ask4, bybit_bid_vol4, bybit_ask_vol4,
        bybit_bid5, bybit_ask5, bybit_bid_vol5, bybit_ask_vol5
        {hyperliquid_columns}
    ) VALUES ({','.join(['?' for _ in range(107 if fetch_spot else 128)])})
    """

    for row in data:
        # 1. Basic data first
        values = [
            row['timestamp'], row['coin'],
            row['binance_last'], row['okx_last'], row['gate_last'], row['bitget_last'], row['bybit_last']
        ]

        # 2. Process regular exchanges first, matching SQL column order
        for exchange in ['binance', 'okx', 'gate', 'bitget', 'bybit']:
            orderbook = row[f'{exchange}_orderbook']
            if orderbook:
                bids = orderbook['bids']
                asks = orderbook['asks']
                for i in range(5):
                    bid_price = bids[i][0] if i < len(bids) else None
                    ask_price = asks[i][0] if i < len(asks) else None
                    bid_volume = bids[i][1] if i < len(bids) else None
                    ask_volume = asks[i][1] if i < len(asks) else None
                    values.extend([bid_price, ask_price, bid_volume, ask_volume])
            else:
                values.extend([None] * 20)

        # 3. Add Hyperliquid data last, only for perp table
        if not fetch_spot:
            values.append(row['hyperliquid_last'])
            orderbook = row['hyperliquid_orderbook']
            if orderbook:
                bids = orderbook['bids']
                asks = orderbook['asks']
                for i in range(5):
                    bid_price = bids[i][0] if i < len(bids) else None
                    ask_price = asks[i][0] if i < len(asks) else None
                    bid_volume = bids[i][1] if i < len(bids) else None
                    ask_volume = asks[i][1] if i < len(asks) else None
                    values.extend([bid_price, ask_price, bid_volume, ask_volume])
            else:
                values.extend([None] * 20)

        try:
            cursor.execute(insert_query, values)
        except pyodbc.DataError as e:
            print(f"Error inserting data for coin {row['coin']}: {e}")
            print(f"Values: {values}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data uploaded to Azure SQL Database at {datetime.now()}")


async def track_prices_and_orderbooks():
    """
    Continuously track prices and orderbooks for all coins across exchanges.
    This function runs in an infinite loop, fetching data every minute.
    """
    while True:
        try:
            azure_data_spot = []
            azure_data_perp = []
            for coin in PERP_COINS:
                fetch_spot = False
                if coin in SPOT_COINS:
                    fetch_spot = True

                perp_result = await fetch_data_for_coin(coin, False)

                if fetch_spot:
                    spot_result = await fetch_data_for_coin(coin, fetch_spot)

                # Append both spot and perp data to azure_data, if they exist
                if fetch_spot and spot_result:
                    azure_data_spot.append(spot_result)
                if perp_result:
                    azure_data_perp.append(perp_result)

            # Upload data to Azure SQL Database only if both spot is true and perp data exist
            if fetch_spot and azure_data_spot:
                upload_to_azure_sql(azure_data_spot, fetch_spot)

            if azure_data_perp:
                upload_to_azure_sql(azure_data_perp, False)

            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Error encountered: {e}. Retrying in 30 seconds...")
            await asyncio.sleep(30)


def main():
    """
    Main function to start the Exchange Data Tracker.
    Sets up the event loop policy for Windows if necessary and runs the tracker.
    """
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(track_prices_and_orderbooks())
    except KeyboardInterrupt:
        print("ExchangeDataTracker shutting down...")


if __name__ == "__main__":
    main()