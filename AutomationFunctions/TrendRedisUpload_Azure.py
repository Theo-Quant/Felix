"""
Code used to pull from the database and get data to calculate bollinger band ranges
"""

import pandas as pd
import redis
import asyncio
from datetime import datetime
import dummy_config as config   # Use dummy_config.py for local testing
import schedule
import time
import traceback
import sys
import numpy as np
import pyodbc

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logger = config.setup_logger('TrendUpload')


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
        Pulls the top 300k rows using pure pyodbc
        '''
        conn = pyodbc.connect(self.CONNECTION_STRING)
        try:
            cursor = conn.cursor()

            # Get the maximum ID
            cursor.execute("SELECT MAX(id) as max_id FROM [dbo].[exchange_dataV2]")
            max_id = cursor.fetchone()[0]

            # Query for the data
            query = f'''
                SELECT 
                    timestamp,
                    coin,
                    okx_bid1,
                    okx_ask1,
                    bybit_bid1,
                    bybit_ask1
                FROM [dbo].[exchange_dataV2]
                WHERE id > {max_id - 300000}
                ORDER BY id ASC
            '''

            cursor.execute(query)

            # Get column names
            columns = [column[0] for column in cursor.description]

            # Fetch all rows
            rows = cursor.fetchall()

            # Convert to pandas DataFrame
            df = pd.DataFrame.from_records(rows, columns=columns)

            # Yield the data in chunks
            for i in range(0, len(df), batch_size):
                yield df.iloc[i:i + batch_size]

        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def calculate_spread(df):
        df['sell_spread'] = (df['okx_bid1'] - df['bybit_ask1']) / df['bybit_ask1'] * 100
        df['buy_spread'] = (df['okx_ask1'] - df['bybit_bid1']) / df['bybit_bid1'] * 100
        return df

    @staticmethod
    def calculate_stats(group, column, window):
        ma = group[column].rolling(window=window).mean()
        std = group[column].rolling(window=window).std()
        return ma, std

    @staticmethod
    def calculate_stats_E(group, column, window):
        ma = group[column].rolling(window=window).mean()
        ewvar = group[column].ewm(span=window, adjust=False).var()
        ewsd = np.sqrt(ewvar)
        return ma, ewsd

    async def calculate_ma_range(self, df, window_m, window_l):
        ma_stats = []
        grouped = df.groupby('coin')
        for coin, group in grouped:
            sell_ma_m, sell_std_m = self.calculate_stats_E(group, 'sell_spread', window_m)
            buy_ma_m, buy_std_m = self.calculate_stats_E(group, 'buy_spread', window_m)
            sell_ma_l, sell_std_l = self.calculate_stats(group, 'sell_spread', window_l)
            buy_ma_l, buy_std_l = self.calculate_stats(group, 'buy_spread', window_l)

            latest_data = group.iloc[-1]
            current_sell_spread = latest_data['sell_spread']
            current_buy_spread = latest_data['buy_spread']

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
                'current_buy_spread': current_buy_spread
            })
        df_stats = pd.DataFrame(ma_stats)
        return df_stats

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
            ma_range_df = await self.calculate_ma_range(all_data, window_m=250, window_l=15)
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
        connection_string = "Driver={SQL SERVER};Server=tcp:theosql.database.windows.net,1433;Database=arbitrage_db_2024-03-22T23-30Z;Uid=THEOsql;Pwd=THEOBullRun2024!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
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
    logger.info("TrendsRedisUpload starting...")

    try:
        update_trends()
    except Exception as e:
        logger.error(f"Error during initial update_trends: {e}")
        logger.error(traceback.format_exc())

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
