import ccxt.pro as ccxtpro  # Note: This is different from ccxt; it supports WebSockets
import pandas as pd
import pyodbc
import asyncio
import config
import traceback

logger = config.setup_logger('TradeLog')

# Initialize the exchanges using credentials from configGB.py
binance = ccxtpro.binance({
    'apiKey': config.BINANCE_API_KEY,
    'secret': config.BINANCE_SECRET_KEY,
    'options': {'adjustForTimeDifference': True}
})

okx = ccxtpro.okx({
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


def clean_binance_trades(trade):
    trade['symbol'] = trade['info']['symbol']
    trade['orderId'] = trade['info']['orderId']
    trade['side'] = trade['info']['side']
    trade['price'] = float(trade['info']['price'])
    trade['qty'] = float(trade['info']['qty'])
    trade['realizedPnl'] = float(trade['info']['realizedPnl'])
    trade['commission'] = float(trade['info']['commission'])
    trade['commissionAsset'] = trade['info']['commissionAsset']
    trade['notional'] = trade['cost']
    trade['datetime'] = pd.to_datetime(trade['timestamp'], unit='ms', utc=True)
    trade['instanceId'] = trade['info']['id']
    trade['CliOrdId'] = trade['clientOrderId']
    return trade


def clean_okx_trades(trade):
    trade['symbol'] = trade['symbol']
    trade['orderId'] = trade['order']
    trade['side'] = trade['side']
    trade['price'] = trade['price']
    trade['qty'] = trade['amount']
    trade['notional'] = trade['cost']
    trade['takerOrMaker'] = trade['takerOrMaker']
    trade['realizedPnl'] = float(trade['info']['fillPnl'])
    trade['commission'] = float(trade['info']['fee'])
    trade['commissionAsset'] = trade['info']['feeCcy']
    trade['datetime'] = pd.to_datetime(trade['timestamp'], unit='ms', utc=True)
    trade['instanceId'] = trade['info']['billId']
    trade['CliOrdId'] = trade['info']['clOrdId']
    return trade


def upload_to_azure(df, table_name):
    if df.empty:
        print(f"No new trades to upload for {table_name}")
        return

    conn = pyodbc.connect(
        f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}')
    cursor = conn.cursor()

    for index, row in df.iterrows():
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE instanceId = ?", row['instanceId'])
        if cursor.fetchone()[0] == 0:
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


async def handle_binance_trades():
    try:
        await binance.load_markets()
        usdt_perp_symbols = [symbol for symbol in binance.markets
                             if 'USDT' in symbol
                             and binance.markets[symbol]['type'] == 'swap'
                             and any(coin in symbol for coin in config.IN_TRADE)]
        for symbol in usdt_perp_symbols:
            print(f"Subscribing to Binance WebSocket for {symbol}...")
            trade_iterator = await binance.watch_my_trades(symbol)
            asyncio.create_task(process_binance_trades(trade_iterator, symbol))  # Process each subscription in its own task
    except Exception as e:
        print(f"Error in Binance WebSocket: {e}")
        logger.error(f"Error in Binance WebSocket: {e}")
        await asyncio.sleep(60)  # Pause before retrying

async def process_binance_trades(trade_iterator, symbol):
    async for trade in trade_iterator:
        trade = clean_binance_trades(trade)
        df = pd.DataFrame([trade])
        upload_to_azure(df, 'BinanceTrades')


async def handle_okx_trades():
    try:
        await okx.load_markets()
        usdt_perp_symbols = [symbol for symbol in okx.markets
                             if 'USDT' in symbol
                             and okx.markets[symbol]['type'] == 'swap'
                             and any(coin in symbol for coin in config.IN_TRADE)]
        for symbol in usdt_perp_symbols:
            print(f"Subscribing to OKX WebSocket for {symbol}...")
            trade_iterator = await okx.watch_my_trades(symbol)
            asyncio.create_task(process_okx_trades(trade_iterator, symbol))  # Process each subscription in its own task
    except Exception as e:
        print(f"Error in OKX WebSocket: {e}")
        logger.error(f"Error in OKX WebSocket: {e}")
        await asyncio.sleep(60)  # Pause before retrying

async def process_okx_trades(trade_iterator, symbol):
    async for trade in trade_iterator:
        trade = clean_okx_trades(trade)
        df = pd.DataFrame([trade])
        upload_to_azure(df, 'OKXTrades')


async def main():
    try:
        logger.info(f"Trading Log restarted")
        await asyncio.gather(handle_binance_trades(), handle_okx_trades())
    except Exception as e:
        logger.error(f"An error occurred: {e} Restarting...")
        traceback.print_exc()
        await asyncio.sleep(60)
        await main()


if __name__ == "__main__":
    asyncio.run(main())
