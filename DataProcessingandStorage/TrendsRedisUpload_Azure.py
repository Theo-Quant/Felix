import pandas as pd
import redis
import asyncio
from datetime import datetime, timedelta
import config
import schedule
import time
import traceback
import sys
import numpy as np
import pyodbc
import db_config
from sqlalchemy import create_engine, text
import requests
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# logger = config.setup_logger('TrendUpload')


class TrendsRedisUpload:
    def __init__(
            self,
            connection_string,
            redis_host='localhost',
            redis_port=6379,
            redis_db=0
    ):
        self.CONNECTION_STRING = connection_string
        self.redis_pool = redis.ConnectionPool(
            host=redis_host,
            port=redis_port,
            db=redis_db
        )
    async def read_data_batch(self, batch_size=10000):
        '''
        Pulls the top 300k rows using pyodbc
        '''
        self.scores = dict()
        self.hyperliquid_funding_rate = dict()  # { symbol: funding }
        self.hyperliquid_open_interest = dict()  # {symbol: open interest}
        self.hyperliquid_day_volume = dict()
        self.hyperliquid_mark_price = dict()
        engine = create_engine(self.CONNECTION_STRING)
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
                df['sell_spread'] = ((df['hyperliquid_bid1'] - df['bybit_ask1']) / df['bybit_ask1']).astype('float')
                df['buy_spread'] = ((df['bybit_ask1'] - df['hyperliquid_bid1']) / df['hyperliquid_bid1']).astype('float')
                averages = df.groupby('coin').apply(self.average_sum_first_ten).dropna()
                latest = df.groupby('coin').apply(lambda x: x.index.max())
                # print("types in dataframe:", df.dtypes)
                # print("df within read_data_batch", df)
                #API calls here
                hyperliquid_url = "https://api.hyperliquid.xyz/info"
                headers = {"Content-Type": "application/json; charset=utf-8"}
                data = {
                    "type": "metaAndAssetCtxs"
                }
                response = self.post_method(hyperliquid_url, headers, data)
                # Yield the data in chunks
                universe = response[0]['universe']
                fundings = response[1]
                # print("universe:", universe)
                for uni, fund in zip(universe, fundings):
                    symbol = uni["name"]
                    fund_rate = float(fund["funding"])
                    self.hyperliquid_funding_rate[f"{symbol}/USDT"] = fund_rate
                    self.hyperliquid_open_interest[f"{symbol}/USDT"] = fund["openInterest"]  # USD
                    self.hyperliquid_day_volume[f"{symbol}/USDT"] = fund["dayNtlVlm"]  # BTC
                    self.hyperliquid_mark_price[f"{symbol}/USDT"] = fund["markPx"]
                self.hyperliquid_day_volume = {sym: float(volume) for sym, volume in self.hyperliquid_day_volume.items()}
                self.hyperliquid_open_interest = {sym: float(volume) * float(self.hyperliquid_mark_price[sym]) for sym, volume in
                                             self.hyperliquid_open_interest.items()}
                symbol = list(averages.index)
                symbol1 = [sym for sym in symbol]
                symbol2 = list(self.hyperliquid_funding_rate.keys())
                common_symbol = self.find_common_elements(symbol1, symbol2)
                print("symbol1:", symbol1)
                print("symbol2:", symbol2)
                print("common_symbol:", common_symbol)
                scores = list()
                max_fund = max([self.hyperliquid_funding_rate[symbol] for symbol in common_symbol])
                max_open_interest = max([self.hyperliquid_open_interest[symbol] for symbol in common_symbol])
                max_day_volume = max([self.hyperliquid_day_volume[symbol] for symbol in common_symbol])
                max_spread = max([averages[symbol] for symbol in common_symbol])
                for symbol in common_symbol:
                    scores.append([symbol, self.calculate_score(averages[symbol], self.hyperliquid_open_interest[symbol],
                                                      self.hyperliquid_day_volume[symbol], max_spread, max_open_interest,
                                                      max_day_volume)])
                self.scores = {sym: float(num) for sym, num in scores}
                # print("df:", df)
                # print("scores:", scores)
                for i in range(0, len(df), batch_size):
                    yield df.iloc[i:i + batch_size]

        finally:
            conn.close()
    def find_common_elements(self, list1, list2):
    # Convert lists to sets
        set1 = set(list1)
        set2 = set(list2)

        # Find the intersection of both sets
        common_elements = set1.intersection(set2)

        # Convert the set back to a list
        return list(common_elements)
    def average_sum_first_ten(self, group):
        if len(group) >= 10:  # Check if group has at least 10 data points
            return group.head(10)['sell_spread'].sum() / 10
        else:
            return None
    @staticmethod
    def calculate_spread(df):
        df['sell_spread'] = (df['hyperliquid_bid1'] - df['bybit_ask1']) / df['bybit_ask1'] * 100
        df['buy_spread'] = (df['bybit_ask1'] - df['hyperliquid_bid1']) / df['hyperliquid_bid1'] * 100
        return df

    @staticmethod
    def calculate_stats(group, column, window):
        ma = group[column].rolling(window=window).mean()
        std = group[column].rolling(window=window).std()
        return ma, std

    @staticmethod
    def calculate_stats_E(group, column, window):
        print("group:", group)
        ma = group[column].rolling(window=window).mean()
        ewvar = group[column].ewm(span=window, adjust=False).var()
        ewsd = np.sqrt(ewvar)
        return ma, ewsd

    @staticmethod
    def calculate_score(spread, open_interest, day_volume, max_spread, max_open_interest, max_day_volume):
        return 0.8 * spread / max_spread + open_interest / max_open_interest * 0.1 + day_volume / max_day_volume * 0.1

    async def calculate_ma_range(self, df, window_m, window_l):
        ma_stats = []
        df.dropna(inplace=True)
        grouped = df.groupby('coin')
        # print("df before the coin group :", df)
        for coin, group in grouped:
            sell_ma_m, sell_std_m = self.calculate_stats_E(group, 'sell_spread', window_m)
            buy_ma_m, buy_std_m = self.calculate_stats_E(group, 'buy_spread', window_m)
            sell_ma_l, sell_std_l = self.calculate_stats(group, 'sell_spread', window_l)
            buy_ma_l, buy_std_l = self.calculate_stats(group, 'buy_spread', window_l)
            latest_data = group.iloc[-1]
            current_sell_spread = latest_data['sell_spread']
            current_buy_spread = latest_data['buy_spread']
            # latest_timestamp = group.apply(lambda x: x.index.max())['index']
            # print("latest_timestamp", latest_timestamp)
            # print("sell_ma_m:", sell_ma_m)
            # print("coin in grouped:", coin)
            ma_stats.append({
                'coin': coin,
                'sell_spread_ma_M': sell_ma_m.iloc[-1],
                'buy_spread_ma_M': buy_ma_m.iloc[-1],
                'sell_spread_sd_M': sell_std_m.iloc[-1],
                'buy_spread_sd_M': buy_std_m.iloc[-1],
                'sell_spread_ma_L': sell_ma_l.iloc[-1],
                'buy_spread_ma_L': buy_ma_l.iloc[-1],
                'sell_spread_sd_L': sell_std_l.iloc[-1],
                'buy_spread_sd_L': buy_std_l.iloc[-1],
                'current_sell_spread': current_sell_spread,
                'current_buy_spread': current_buy_spread,
                'score': self.scores[coin],
                "hyperliquid_funding_rate": self.hyperliquid_funding_rate[coin],
                "hyperliquid_open_interest": self.hyperliquid_open_interest[coin],
                "hyperliquid_day_volume": self.hyperliquid_day_volume[coin],
            })
        df_stats = pd.DataFrame(ma_stats)
        df_stats = df_stats.sort_values(by='score', ascending=False)
        df_stats.to_csv("ma_stats.csv", index=False)
        return df_stats
    def post_method(self, url, headers, data):
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
    def upload_to_redis(self, df):
        redis_client = redis.Redis(connection_pool=self.redis_pool)

        with redis_client.pipeline() as pipe:
            pipe.delete('trend_data')

            for _, row in df.iterrows():
                coin = row['coin']
                row_json = row.drop('coin').to_json()
                pipe.hset('trend_data', coin, row_json)

            pipe.execute()

        print(f"Uploaded {len(df)} records to Redis at {datetime.now()}")

        print("Verifying upload...")
        keys = redis_client.hkeys('trend_data')
        print(f"Number of keys in 'trend_data': {len(keys)}")

        if keys:
            first_key = keys[0].decode('utf-8')
            first_value = redis_client.hget('trend_data', first_key).decode('utf-8')
            print(f"Sample record - {first_key}: {first_value}")

    async def process_and_upload(self):
        all_data = pd.concat([chunk async for chunk in self.read_data_batch() if not chunk.empty])
        if not all_data.empty:
            all_data = self.calculate_spread(all_data)
            ma_range_df = await self.calculate_ma_range(all_data, window_m=144, window_l=15)
            print(ma_range_df)
            self.upload_to_redis(ma_range_df)
            print(f"Uploaded to Redis at {datetime.now()}")
        else:
            print("No data to process.")

    async def run(self, interval=180):
        while True:
            await self.process_and_upload()
            await asyncio.sleep(interval)


async def update_trends_redis(
        connection_string,
        redis_host='localhost',
        redis_port=6379,
        redis_db=0
):
    uploader = TrendsRedisUpload(connection_string, redis_host, redis_port, redis_db)
    await uploader.process_and_upload()


def update_trends():
    try:
        connection_string = db_config.connection_string_dash
        asyncio.run(update_trends_redis(connection_string))
    except Exception as e:
        print(f"Error in update_trends: {e}")
        print(traceback.format_exc())


def run_schedule():
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            print(f"Error in schedule loop: {e}")
            print(traceback.format_exc())
            print("Restarting schedule loop...")
            time.sleep(5)


def main():
    # logger.info("TrendsRedisUpload starting...")

    try:
        update_trends()
    except Exception as e:
        # logger.error(f"Error during initial update_trends: {e}")
        # logger.error(traceback.format_exc())
        pass

    schedule.every(1).minutes.do(update_trends)

    while True:
        try:
            run_schedule()
        except KeyboardInterrupt:
            logger.info("TrendsRedisUpload shutting down...")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            logger.error(traceback.format_exc())
            logger.info("Restarting main loop...")
            time.sleep(5)

    logger.info("TrendsRedisUpload has ended. This message should not appear unless intentionally stopped.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Fatal error in main: {e}")
        logger.critical(traceback.format_exc())
    finally:
        logger.info("Script has exited. This message should not appear unless intentionally stopped.")