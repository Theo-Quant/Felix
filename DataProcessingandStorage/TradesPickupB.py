import ccxt
import pandas as pd
import pyodbc
import schedule
import time
from datetime import datetime, timedelta
import config
import sys
import traceback

logger = config.setup_logger('TradeLog')

# Initialize the exchanges using credentials from configGB.py
binance = ccxt.binance({
    'apiKey': config.BINANCE_API_KEY,
    'secret': config.BINANCE_SECRET_KEY,
    'options': {'adjustForTimeDifference': True}
})

okx = ccxt.okx({
    'apiKey': config.OKX_API_KEY,
    'secret': config.OKX_SECRET_KEY,
    'password': config.OKX_PASSPHRASE,
})

# Azure SQL Database connection details
driver = '{ODBC Driver 17 for SQL Server}'
server = 'theosql.database.windows.net'
database = 'arbitrage_db_2024-03-22T23-30Z'
username = 'THEOsql'
password = 'THEOBullRun2024!'

def fetch_usdt_perp_trades(exchange, exchange_id, start_time, end_time, symboln):
    all_trades = []
    exchange.load_markets()
    usdt_perp_symbols = [symbol for symbol in exchange.markets if symbol == f'{symboln}/USDT:USDT' and exchange.markets[symbol]['type'] == 'swap']
    for symbol in usdt_perp_symbols:
        print(f"Fetching trades for {symbol} on {exchange_id}...")
        try:
            trades = exchange.fetch_my_trades(symbol=symbol, params={
                'since': int(start_time.timestamp() * 1000),
                'until': int(end_time.timestamp() * 1000)
            })
            if trades:
                for trade in trades:
                    if exchange_id == 'binance':
                        order_id = trade.get('order')
                        if order_id:
                            order_details = exchange.fetch_order(order_id, symbol)
                            trade['clientOrderId'] = order_details.get('clientOrderId')
                    all_trades.append(trade)
        except Exception as e:
            print(f"Error fetching trades for {symbol} on {exchange_id}: {e}")
    return all_trades

# Function to clean up the Binance DataFrame
def clean_binance_trades_df(df):
    if not df.empty:
        df['symbol'] = df['info'].apply(lambda x: x['symbol'])
        df['orderId'] = df['info'].apply(lambda x: x['orderId'])
        df['side'] = df['info'].apply(lambda x: x['side'])
        df['price'] = df['info'].apply(lambda x: float(x['price']))
        df['qty'] = df['info'].apply(lambda x: float(x['qty']))
        df['realizedPnl'] = df['info'].apply(lambda x: float(x['realizedPnl']))
        df['commission'] = df['info'].apply(lambda x: float(x['commission']))
        df['commissionAsset'] = df['info'].apply(lambda x: x['commissionAsset'])
        df['notional'] = df['cost']
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df['instanceId'] = df['info'].apply(lambda x: x['id'])
        df['CliOrdId'] = df['clientOrderId']
        df.drop(columns=['fees', 'fee'], inplace=True)
        new_order = ['datetime', 'symbol', 'orderId', 'takerOrMaker', 'side', 'price', 'qty', 'notional', 'commission',
                     'commissionAsset', 'realizedPnl', 'info', 'instanceId', 'CliOrdId']
        df = df[new_order]
    return df

# Function to clean up the OKX DataFrame
def clean_okx_trades_df(df):
    if not df.empty:
        df['symbol'] = df['symbol']
        df['orderId'] = df['order']
        df['side'] = df['side']
        df['price'] = df['price']
        df['qty'] = df['amount']
        df['notional'] = df['cost']
        df['takerOrMaker'] = df['takerOrMaker']
        df['realizedPnl'] = df['info'].apply(lambda x: float(x['fillPnl']))
        df['commission'] = df['info'].apply(lambda x: float(x['fee']))
        df['commissionAsset'] = df['info'].apply(lambda x: x['feeCcy'])
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df['instanceId'] = df['info'].apply(lambda x: x['billId'])
        df['CliOrdId'] = df['info'].apply(lambda x: x['clOrdId'])
        df.drop(columns=['fees', 'fee'], inplace=True)
        new_order = ['datetime', 'symbol', 'orderId', 'takerOrMaker', 'side', 'price', 'qty', 'notional', 'commission',
                     'commissionAsset', 'realizedPnl', 'info', 'instanceId', 'CliOrdId']
        df = df[new_order]
        print(df.head(5))
        print(df.tail(5))
    return df

# Function to upload DataFrame to Azure SQL
def upload_to_azure(df, table_name):
    if df.empty:
        print(f"No new trades to upload for {table_name}")
        return

    conn = pyodbc.connect(
        f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}')
    cursor = conn.cursor()

    for index, row in df.iterrows():
        # Check if the record exists
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE instanceId = ?", row['instanceId'])
        if cursor.fetchone()[0] == 0:
            # Insert the record if it doesn't exist
            cursor.execute(f"""
                INSERT INTO {table_name} (datetime, symbol, orderId, takerOrMaker, side, price, qty, notional, commission, commissionAsset, realizedPnl, info, instanceId, CliOrdId)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row['datetime'], row['symbol'], row['orderId'], row['takerOrMaker'],
                   row['side'], row['price'], row['qty'], row['notional'], row['commission'], row['commissionAsset'],
                   row['realizedPnl'], str(row['info']), row['instanceId'], row['CliOrdId'])

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data uploaded to {table_name}")

def run_trading_bot(start_time, end_time, symbol):
    print(f"Fetching trades from {start_time} to {end_time}")

    print("Fetching Binance USDT perpetual futures trades...")
    binance_trades = fetch_usdt_perp_trades(binance, 'binance', start_time, end_time, symbol)

    binance_df = pd.DataFrame(binance_trades)
    print(binance_df)

    binance_df = clean_binance_trades_df(binance_df)

    upload_to_azure(binance_df, 'BinanceTrades')


def main():
    start_datetime = datetime(2024, 11, 6, 13, 0, 0)
    end_datetime = datetime(2024, 11, 6, 16, 20, 0)
    # run_trading_bot(start_datetime, end_datetime, '1000CAT')

    current_start = start_datetime
    while current_start < end_datetime:
        current_end = current_start + timedelta(seconds=600)
        if current_end > end_datetime:
            current_end = end_datetime

        print(f"Processing interval: {current_start} to {current_end}")
        run_trading_bot(current_start, current_end, 'NEIRO')
        current_start = current_end


if __name__ == "__main__":
    main()