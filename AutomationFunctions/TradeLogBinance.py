import ccxt
import pandas as pd
import pyodbc
import schedule
import time
from datetime import datetime, timedelta
import config
import sys
import traceback

logger = config.setup_logger('BinanceTradeLog')

# Initialize the Binance exchange using credentials from configGB.py
binance = ccxt.binance({
    'apiKey': config.BINANCE_API_KEY,
    'secret': config.BINANCE_SECRET_KEY,
    'options': {'adjustForTimeDifference': True}
})

# Azure SQL Database connection details
driver = '{ODBC Driver 17 for SQL Server}'
server = 'theosql.database.windows.net'
database = 'arbitrage_db_2024-03-22T23-30Z'
username = 'THEOsql'
password = 'THEOBullRun2024!'

IN_TRADE = config.IN_TRADE + ['NEIROETH', 'REEF', 'CKB', 'BANANA', 'GLM', 'KAS', 'ENA', '1000CAT']
# Format the symbols for Binance USDT perpetual futures

def fetch_usdt_perp_trades(exchange, since):
    all_trades = []
    exchange.load_markets()
    usdt_perp_symbols = [f"{symbol}/USDT:USDT" for symbol in IN_TRADE]
    for symbol in usdt_perp_symbols:
        print(f"Fetching trades for {symbol} on Binance...")
        try:
            trades = exchange.fetch_my_trades(symbol=symbol, params={'since': since})
            if trades:
                for trade in trades:
                    order_id = trade.get('order')
                    if order_id:
                        order_details = exchange.fetch_order(order_id, symbol)
                        trade['clientOrderId'] = order_details.get('clientOrderId')
                    all_trades.append(trade)
        except Exception as e:
            print(f"Error fetching trades for {symbol} on Binance: {e}")
    return all_trades

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


def upload_to_azure(df, table_name):
    if df.empty:
        print(f"No new trades to upload for {table_name}")
        return

    try:
        conn = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}')
        cursor = conn.cursor()

        # Calculate the date 1 day ago
        one_day_ago = datetime.now(df['datetime'].dt.tz) - timedelta(days=1)

        # Filter the DataFrame to include only trades from the last day
        df_filtered = df[df['datetime'] >= one_day_ago]

        if df_filtered.empty:
            print(f"No new trades within the last day for {table_name}")
            return

        # Get all instanceIds from the filtered DataFrame
        instance_ids = df_filtered['instanceId'].tolist()

        if not instance_ids:
            print(f"No instance IDs to process for {table_name}")
            return

        # Process in batches of 1000 instanceIds
        batch_size = 1000
        total_new_records = 0

        for i in range(0, len(instance_ids), batch_size):
            batch_ids = instance_ids[i:i + batch_size]

            placeholders = ','.join(['?' for _ in batch_ids])
            query = f"""
                DECLARE @one_day_ago DATETIME = ?;
                SELECT instanceId 
                FROM {table_name} 
                WHERE instanceId IN ({placeholders})
                AND datetime >= @one_day_ago
            """

            params = [one_day_ago] + batch_ids
            cursor.execute(query, params)

            existing_ids = set(row.instanceId for row in cursor.fetchall())

            # Insert only the new records for this batch
            new_records_count = 0
            for _, row in df_filtered[df_filtered['instanceId'].isin(batch_ids)].iterrows():
                if row['instanceId'] not in existing_ids:
                    cursor.execute(f"""
                        INSERT INTO {table_name} (datetime, symbol, orderId, takerOrMaker, side, price, qty, notional, commission, commissionAsset, realizedPnl, info, instanceId, CliOrdId)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, row['datetime'], row['symbol'], row['orderId'], row['takerOrMaker'],
                                   row['side'], row['price'], row['qty'], row['notional'], row['commission'],
                                   row['commissionAsset'],
                                   row['realizedPnl'], str(row['info']), row['instanceId'], row['CliOrdId'])
                    new_records_count += 1

            total_new_records += new_records_count
            print(f"Processed batch {i // batch_size + 1}. {new_records_count} new records inserted.")

        conn.commit()
        print(f"Data uploaded to {table_name}. Total {total_new_records} new records inserted.")

    except pyodbc.Error as e:
        print(f"Database error: {str(e)}")
        print(f"SQL Statement: {query}")
        print(f"Parameters: {params}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def run_trading_bot():
    now = datetime.now()
    since = now - timedelta(hours=3, minutes=30)
    since_timestamp = int(since.timestamp() * 1000)

    print("Fetching Binance USDT perpetual futures trades...")
    binance_trades = fetch_usdt_perp_trades(binance, since_timestamp)

    binance_df = pd.DataFrame(binance_trades)
    binance_df = clean_binance_trades_df(binance_df)

    upload_to_azure(binance_df, 'BinanceTrades')

def main():
    while True:
        try:
            logger.info(f"Binance Trading Log restarted")
            schedule.every(1).minutes.do(run_trading_bot)
            run_trading_bot()

            while True:
                schedule.run_pending()
                time.sleep(1)
        except Exception as e:
            logger.error(f"An error occurred: {e} Restarting in 60 seconds...")
            traceback.print_exc()
            time.sleep(60)
            continue

if __name__ == "__main__":
    main()