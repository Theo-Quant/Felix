from dotenv import load_dotenv
import os
import requests
import time
import telebot
import asyncio
import websockets
import json
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from config import *
import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt

load_dotenv()
API_TOKEN= os.getenv('BOT_TOKEN', "")
# Constants
CHAT_ID = '7346012683'
BOT_TOKEN = API_TOKEN
SPREAD_THRESHOLD = 0.02  # Example threshold for spread (2%)
FUNDING_THRESHOLD = 0.00002  # Example threshold for funding (2%)
AZURE_SQL_SERVER = os.getenv('AZURE_SQL_SERVER', '')
AZURE_SQL_DATABASE = os.getenv('AZURE_SQL_DATABASE', '')
AZURE_SQL_USERNAME = os.getenv('AZURE_SQL_USERNAME', '')
AZURE_SQL_PASSWORD = os.getenv('AZURE_SQL_PASSWORD', '')
AZURE_SQL_CONNECTION_STRING = f"mssql+pyodbc://{AZURE_SQL_USERNAME}:{AZURE_SQL_PASSWORD}@{AZURE_SQL_SERVER}/{AZURE_SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
# Initialize the bot
bot = telebot.TeleBot(BOT_TOKEN)
hyperliquid_funding_rate = dict()  # { symbol: funding }
hyperliquid_open_interest = dict()  # {symbol: open interest}
hyperliquid_day_volume = dict()
hyperliquid_mark_price = dict()
def connect_to_db():
    try:
        engine = create_engine(AZURE_SQL_CONNECTION_STRING)
        print("Connected to SQL Server Database")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
def get_market_data():
    # Example API call to get market data from Hyperliquid/Bybit
    global hyperliquid_funding_rate
    global hyperliquid_open_interest
    global hyperliquid_day_volume
    global hyperliquid_mark_price
    engine = create_engine(AZURE_SQL_CONNECTION_STRING)
    try:

        # Get the maximum ID
        # Query for the data
        two_weeks = 150000
        query = text(f"""
                SELECT 
                    a.coin,
                    a.timestamp,
                    a.hyperliquid_bid1, 
                    a.hyperliquid_ask1, 
                    b.bybit_bid1, 
                    b.bybit_ask1
                FROM 
                    (SELECT TOP {two_weeks} coin, timestamp, hyperliquid_bid1, hyperliquid_ask1 
                     FROM exchange_dataV2 
                     ORDER BY id DESC) a
                INNER JOIN 
                    (SELECT TOP {two_weeks} coin, timestamp, bybit_bid1, bybit_ask1 
                     FROM exchange_data_spot 
                     ORDER BY id DESC) b
                ON a.coin = b.coin AND a.timestamp = b.timestamp
                """)

        with engine.connect() as conn:
            print("connected to server")
            result = conn.execute(query).fetchall()
            print("Get the result ")
            # Get column names
            df = pd.DataFrame(result,
                              columns=['coin', 'timestamp', 'hyperliquid_bid1', 'hyperliquid_ask1', 'bybit_bid1',
                                       'bybit_ask1'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            two_weeks_ago = datetime.now() - timedelta(days=14)
            df = df[df['timestamp'] >= two_weeks_ago]
            df.set_index('timestamp', inplace=True)
            if df.isna().any().any():
                df = df.dropna(axis=0)
                df.reset_index(inplace=True)
            # print("df: before spreads created",df)
            df['sell_spread'] = ((df['hyperliquid_bid1'] - df['bybit_ask1']) / df['bybit_ask1']).astype('float') * 100
            df['buy_spread'] = ((df['hyperliquid_ask1'] - df['bybit_bid1']) / df['bybit_bid1']).astype('float')* 100
            averages = df.groupby('coin').apply(average_sum_first_ten).dropna()
            latest = df.groupby('coin').apply(lambda x: x.index.max())
            # print("types in dataframe:", df.dtypes)
            # print("df within read_data_batch", df)
            # API calls here
            hyperliquid_url = "https://api.hyperliquid.xyz/info"
            headers = {"Content-Type": "application/json; charset=utf-8"}
            data = {
                "type": "metaAndAssetCtxs"
            }
            response = post_method(hyperliquid_url, headers, data)
            # Yield the data in chunks
            universe = response[0]['universe']
            fundings = response[1]
            # print("universe:", universe)
            for uni, fund in zip(universe, fundings):
                symbol = uni["name"]
                fund_rate = float(fund["funding"])
                hyperliquid_funding_rate[f"{symbol}/USDT"] = fund_rate
                hyperliquid_open_interest[f"{symbol}/USDT"] = fund["openInterest"]  # USD
                hyperliquid_day_volume[f"{symbol}/USDT"] = fund["dayNtlVlm"]  # BTC
                hyperliquid_mark_price[f"{symbol}/USDT"] = fund["markPx"]
            hyperliquid_day_volume = {sym: float(volume) for sym, volume in hyperliquid_day_volume.items()}
            hyperliquid_open_interest = {sym: float(volume) * float(hyperliquid_mark_price[sym]) for
                                              sym, volume in
                                              hyperliquid_open_interest.items()}
            symbol = list(averages.index)
            symbol1 = [sym for sym in symbol]
            symbol2 = list(hyperliquid_funding_rate.keys())
            return df
    finally:
        conn.close()
    return None
def find_common_elements(list1, list2):
    # Convert lists to sets
        set1 = set(list1)
        set2 = set(list2)

        # Find the intersection of both sets
        common_elements = set1.intersection(set2)

        # Convert the set back to a list
        return list(common_elements)
def average_sum_first_ten(group):
    if len(group) >= 10:  # Check if group has at least 10 data points
        return group.head(10)['sell_spread'].sum() / 10
    else:
        return None
def post_method(url, headers, data):
    try:
        # Send POST request
        resp = requests.post(url=url, headers=headers, json=data)

        # Check response status codes
        if resp.status_code == 200:
            # Parse JSON from the response
            response = resp.json()
            return response
        elif resp.status_code == 404:
            return "Not Found."
        else:
            return f"Failed with status code: {resp.status_code}"
    except requests.exceptions.RequestException as e:
        # Return error message for connection-related exceptions
        return f"Error during the request: {e}"
def check_alerts(data):
    global hyperliquid_funding_rate
    global hyperliquid_open_interest
    global hyperliquid_day_volume
    global hyperliquid_mark_price
    print("data:", data)
    print(hyperliquid_funding_rate)
    grouped = data.groupby('coin')
    for coin, group in grouped:
        spread = group.iloc[-1]['sell_spread'] # Adjust field names based on actual API response
        funding_rate = hyperliquid_funding_rate[coin]
        if spread > SPREAD_THRESHOLD:
            send_alert(f"Spread Alert for {coin}: {spread:.2%}")

        if funding_rate > FUNDING_THRESHOLD:
            send_alert(f"Funding Rate Alert for {coin}: {funding_rate:.2%}")


def send_alert(message):
    bot.send_message(chat_id=CHAT_ID, text=message)


def main():
    while True:
        market_data = get_market_data()
        print("feeding into market data")
        check_alerts(market_data)
        time.sleep(60)  # Check every 60 seconds


if __name__ == '__main__':
    main()