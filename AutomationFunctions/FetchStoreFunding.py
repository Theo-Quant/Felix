import os
import ccxt
import logging
# import config
import dummy_config as config
from datetime import datetime, timedelta
import pandas as pd
import pyodbc
import schedule
import time
from dotenv import load_dotenv

# Initialize exchange clients
# binance = ccxt.binance({
#     'apiKey': config.BINANCE_API_KEY,
#     'secret': config.BINANCE_SECRET_KEY,
#     'enableRateLimit': True,
#     'options': {
#         'defaultType': 'future'
#     }
# })

okx = ccxt.okx({
    'apiKey': config.OKX_API_KEY,
    'secret': config.OKX_SECRET_KEY,
    'password': config.OKX_PASSPHRASE,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap'
    }
})

# gate = ccxt.gateio({
#     'apiKey': config.GATE_API_KEY,
#     'secret': config.GATE_SECRET_KEY,
#     'enableRateLimit': True,
#     'options': {
#         'defaultType': 'swap'
#     }
# })

bybit = ccxt.bybit({
    'apiKey': config.BYBIT_API_KEY,
    'secret': config.BYBIT_SECRET_KEY,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future'
    }
})

# Azure SQL Database connection details
load_dotenv()
driver = os.getenv('THEO_DB_DRIVER')
server = os.getenv('THEO_DB_SERVER')
database = os.getenv('THEO_DB_DATABASE')
username = os.getenv('THEO_DB_UID')
password = os.getenv('THEO_DB_PASSWORD')

def connect_to_database():
    connection_string = (
        f'DRIVER={driver};'
        f'SERVER=tcp:{server},1433;'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        'Encrypt=yes;'
        'TrustServerCertificate=no;'
        'Connection Timeout=30;'
    )
    return pyodbc.connect(connection_string)


def create_funding_data_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='funding_data' AND xtype='U')
    CREATE TABLE funding_data (
        id INT IDENTITY(1,1) PRIMARY KEY,
        funding_time DATETIME,
        exchange VARCHAR(50),
        coin VARCHAR(50),
        funding FLOAT
    )
    """)
    conn.commit()
    cursor.close()


def insert_funding_data(conn, funding_time, exchange, coin, funding):
    cursor = conn.cursor()
    query = """
    INSERT INTO funding_data (funding_time, exchange, coin, funding)
    VALUES (?, ?, ?, ?)
    """
    cursor.execute(query, (funding_time, exchange, coin, funding))
    conn.commit()
    cursor.close()


def get_funding_payout(exchange, max_retries=3, retry_delay=5):
    for attempt in range(max_retries):
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=3)
            since = int(start_time.timestamp() * 1000)
            until = int(end_time.timestamp() * 1000)

            params = {'until': until}

            # Special handling for Gate
            if exchange.id == 'gateio':
                since = int(start_time.timestamp())  # Convert to seconds for Gate.io
                params['from'] = since
            else:
                params['since'] = since

            funding_history = exchange.fetch_funding_history(None, since, params=params)
            print(f"Fetched {len(funding_history)} funding entries from {exchange.id}")

            funding_payouts = {}
            for entry in funding_history:
                symbol = entry['symbol']
                if exchange.id == 'bybit':
                    amount = float(entry['info']['execFee'])
                else:
                    amount = entry['amount']
                timestamp = entry['timestamp']

                if symbol not in funding_payouts:
                    funding_payouts[symbol] = []

                funding_payouts[symbol].append({
                    'amount': amount,
                    'timestamp': timestamp,
                })

            return funding_payouts
        except Exception as e:
            if exchange.id == 'okx' and attempt < max_retries - 1:
                print(f"Error fetching funding history from OKX (attempt {attempt + 1}): {e}")
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Error fetching funding history from {exchange.id}: {e}")
                return {}

    print(f"Failed to fetch funding history from OKX after {max_retries} attempts")
    return {}


def create_funding_df_and_store(okx, bybit):
    okx_funding = get_funding_payout(okx)
    # binance_funding = get_funding_payout(binance)
    # gate_funding = get_funding_payout(gate)
    bybit_funding = get_funding_payout(bybit)
    print(okx_funding)
    print(bybit_funding)
    # print(binance_funding)
    # print(gate_funding)

    conn = connect_to_database()
    create_funding_data_table(conn)

    all_data = []

    for exchange, funding_data in [('OKX', okx_funding), ('Bybit', bybit_funding)]: #('Binance', binance_funding), ('Gate', gate_funding)]:
        for symbol, entries in funding_data.items():
            coin = symbol.split(':')[0] if ':' in symbol else symbol.split('/')[0]
            for entry in entries:
                funding_time = datetime.fromtimestamp(entry['timestamp'] / 1000)
                funding_amount = entry['amount']

                insert_funding_data(conn, funding_time, exchange, coin, funding_amount)

                all_data.append({
                    'timestamp': entry['timestamp'],
                    'symbol': symbol,
                    'coin': coin,
                    'exchange': exchange,
                    'funding': funding_amount
                })

    conn.close()

    df = pd.DataFrame(all_data)
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.sort_values(['symbol', 'timestamp'])

    return df


def run_funding_update():
    print(f"Running funding update at {datetime.now()}")
    funding_df = create_funding_df_and_store(okx, bybit)

    if not funding_df.empty:
        print(funding_df)
        print(f"Total funding: {funding_df['funding'].sum()}")
    else:
        print("No funding data to display.")


def main():
    run_funding_update()
    schedule.every().day.at("08:01").do(run_funding_update)
    schedule.every().day.at("12:01").do(run_funding_update)
    schedule.every().day.at("16:01").do(run_funding_update)
    schedule.every().day.at("20:01").do(run_funding_update)
    schedule.every().day.at("00:01").do(run_funding_update)
    schedule.every().day.at("04:01").do(run_funding_update)

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    main()