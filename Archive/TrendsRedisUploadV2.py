import sqlite3
import pandas as pd
import redis
import asyncio
from datetime import datetime
import config
import schedule
import time
import traceback
import sys
import numpy as np

"""
Fix the warning for concating NA values in a df.
"""
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logger = config.setup_logger('TrendUpload')


class TrendsRedisUpload:
    def __init__(self, database_path, redis_host='localhost', redis_port=6379, redis_db=0):
        self.DATABASE = database_path
        self.db_pool = sqlite3.connect(self.DATABASE, check_same_thread=False)
        self.redis_pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=redis_db)

    async def read_data_batch(self, batch_size=10000):
        query = 'SELECT * FROM "spreads" WHERE timestamp > "2024-07-02 07:28:00"'
        for chunk in pd.read_sql_query(query, self.db_pool, chunksize=batch_size):
            yield chunk

    @staticmethod
    def calculate_spread(df):
        df['sell_spread'] = (df['okx_bid'] - df['binance_ask']) / df['binance_ask'] * 100
        df['buy_spread'] = (df['okx_ask'] - df['binance_bid']) / df['binance_bid'] * 100
        return df

    @staticmethod
    def calculate_stats(group, column, window):
        ma = group[column].rolling(window=window).mean()
        std = group[column].rolling(window=window).std()
        return ma, std

    @staticmethod
    def calculate_stats_E(group, column, window):
        # Calculate the simple moving average (MA)
        ma = group[column].rolling(window=window).mean()
        # Calculate the Exponentially Weighted Standard Deviation (EWSD)
        ewvar = group[column].ewm(span=window, adjust=False).var()
        ewsd = np.sqrt(ewvar)

        return ma, ewsd

    async def calculate_ma_range(self, df, windowM, windowL):
        ma_stats = []
        grouped = df.groupby('coin')
        for coin, group in grouped:
            sell_ma_M, sell_std_M = self.calculate_stats_E(group, 'sell_spread', windowM)
            buy_ma_M, buy_std_M = self.calculate_stats_E(group, 'buy_spread', windowM)
            sell_ma_L, sell_std_L = self.calculate_stats(group, 'sell_spread', windowL)
            buy_ma_L, buy_std_L = self.calculate_stats(group, 'buy_spread', windowL)

            # Get the most recent data
            latest_data = group.iloc[-1]
            current_sell_spread = latest_data['sell_spread']
            current_buy_spread = latest_data['buy_spread']

            ma_stats.append({
                'coin': coin,
                'sell_spread_ma_M': sell_ma_M.iloc[-1],
                'buy_spread_ma_M': buy_ma_M.iloc[-1],
                'sell_spread_sd_M': sell_std_M.iloc[-1],
                'buy_spread_sd_M': buy_std_M.iloc[-1],
                'sell_spread_ma_L': sell_ma_L.iloc[-1],
                'buy_spread_ma_L': buy_ma_L.iloc[-1],
                'sell_spread_sd_L': sell_std_L.iloc[-1],
                'buy_spread_sd_L': buy_std_L.iloc[-1],
                'current_sell_spread': current_sell_spread,
                'current_buy_spread': current_buy_spread
            })
        df_stats = pd.DataFrame(ma_stats)
        return df_stats

    def upload_to_redis(self, df):
        redis_client = redis.Redis(connection_pool=self.redis_pool)

        # Start a transaction
        with redis_client.pipeline() as pipe:
            # Delete the existing 'trend_data' hash
            pipe.delete('trend_data')

            # Upload new data
            for _, row in df.iterrows():
                coin = row['coin']
                # Convert the row to a JSON string, excluding the 'coin' column
                row_json = row.drop('coin').to_json()
                pipe.hset('trend_data', coin, row_json)

            # Execute all commands in the pipeline
            pipe.execute()

        print(f"Uploaded {len(df)} records to Redis at {datetime.now()}")

        # Verify the upload
        print("Verifying upload...")
        keys = redis_client.hkeys('trend_data')
        print(f"Number of keys in 'trend_data': {len(keys)}")

        # Print the first record as an example
        if keys:
            first_key = keys[0].decode('utf-8')
            first_value = redis_client.hget('trend_data', first_key).decode('utf-8')
            print(f"Sample record - {first_key}: {first_value}")

    async def process_and_upload(self):
        all_data = pd.concat([chunk async for chunk in self.read_data_batch()])
        print(all_data)
        if not all_data.empty:
            all_data = self.calculate_spread(all_data)
            ma_range_df = await self.calculate_ma_range(all_data, windowM=500, windowL=35)
            print(ma_range_df)
            # self.upload_to_redis(ma_range_df)  # Call the upload_to_redis function here
            print(f"Uploaded to Redis at {datetime.now()}")
        else:
            print("No data to process.")

    async def run(self, interval=180):
        while True:
            await self.process_and_upload()
            await asyncio.sleep(interval)  # Run every 3 minutes by default


# This function can be called from another script
async def update_trends_redis(database_path, redis_host='localhost', redis_port=6379, redis_db=0):
    uploader = TrendsRedisUpload(database_path, redis_host, redis_port, redis_db)
    await uploader.process_and_upload()


def update_trends():
    try:
        database_path = r'/CrossExchangeMarketMaking/LiveTestingV2/FundingRateArbitrage/arbitrage_data.db'
        asyncio.run(update_trends_redis(database_path))
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
            time.sleep(5)  # Wait for 5 seconds before restarting


def main():
    logger.info("TrendsRedisUpload starting...")

    # Run once immediately on startup
    try:
        update_trends()
    except Exception as e:
        logger.error(f"Error during initial update_trends: {e}")
        logger.error(traceback.format_exc())

    # Schedule to run every 1 minutes
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