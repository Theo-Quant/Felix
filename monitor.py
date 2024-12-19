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
import schedule

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
            common_symbol = find_common_elements(symbol1, symbol2)
            # print("symbol1:", symbol1)
            # print("symbol2:", symbol2)
            # print("common_symbol:", common_symbol)
            # you only need this for a score
            scores = list()
            max_fund = max([hyperliquid_funding_rate[symbol] for symbol in common_symbol])
            max_open_interest = max([hyperliquid_open_interest[symbol] for symbol in common_symbol])
            max_day_volume = max([hyperliquid_day_volume[symbol] for symbol in common_symbol])
            max_spread = max([averages[symbol] for symbol in common_symbol])
            for symbol in common_symbol:
                scores.append([symbol, calculate_score(averages[symbol], hyperliquid_open_interest[symbol],
                                                            hyperliquid_day_volume[symbol], max_spread,
                                                            max_open_interest,
                                                            max_day_volume)])
            scores = sorted(scores, key=lambda x: x[1], reverse=True)
            top_five = [sym for sym, score in scores[:5]]
            scores = {sym:score for sym, score in scores[:5]}
            df = df[df['coin'].isin(top_five)]
            return df, scores
    finally:
        conn.close()
    return None

def calculate_score(spread, open_interest, day_volume, max_spread, max_open_interest, max_day_volume):
    return 0.8 * spread / max_spread + open_interest / max_open_interest * 0.1 + day_volume / max_day_volume * 0.1
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
def check_alerts(data, top_five:dict):
    global hyperliquid_funding_rate
    global hyperliquid_open_interest
    global hyperliquid_day_volume
    global hyperliquid_mark_price
    # print("data:", data)
    # print(hyperliquid_funding_rate)
    symbol = [symb for symb, score in top_five.items()]
    for coin in symbol:
        funding_rate = hyperliquid_funding_rate[coin]
        latest_data = data[data['coin'] == coin]
        latest_data.sort_values(by = ['timestamp'], ascending = False, inplace = True)
        latest_data.reset_index(inplace = True)
        print('latest_data', latest_data)
        spread = latest_data.iloc[0]['sell_spread']
        send_alert(f"Spread Alert for {coin}: {spread:.5%} and Funding Rate Alert for {coin}: {funding_rate:.5%}")

def send_alert(message):
    bot.send_message(chat_id=CHAT_ID, text=message)
def job():
    market_data, top_five_score= get_market_data()
    print("feeding into market data")
    check_alerts(market_data, top_five_score)

def main():
    schedule.every().hour.at(":00").do(job)
    while True:
        schedule.run_pending()
        # job()
        time.sleep(1)

if __name__ == '__main__':
    main()