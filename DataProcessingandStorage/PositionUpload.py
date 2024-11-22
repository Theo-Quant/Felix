import redis
import json
import ccxt
import config
import schedule
import time
import sys
import traceback
import os
import pyodbc

logger = config.setup_logger('PositionUpload')

"""
Looks through all bot_params in the redis.
For each bot id in the selection, it gets the mark price and total position size.
It then pulls the total position size from GB_bot_params.
It uploads the new mark price to redis, and based on the total position size of both bots it sets the rules for the trading bot.
"""

class MarketDataUpdater:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = redis.Redis(host=host, port=port, db=db)
        # notional_adjustment comes from risk_management class
        # only_exit comes from the hedging bot indicating we have run out of margin to enter more positions.
        self.total_position_threshold = 2000000 * float(self.redis_client.get('only_exit'))     # * float(self.redis_client.get('notional_adjustment'))
        self.reset_threshold = self.total_position_threshold * 0.99
        self.default_max_notional = 20000
        self.exchange = ccxt.okx({
            'apiKey': config.OKX_API_KEY,
            'secret': config.OKX_SECRET_KEY,
            'password': config.OKX_PASSPHRASE,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'swap'
            }
        })

    def insert_into_azure_sql(self, bot_id, mark_price, position_size):
        conn_str = (
            f'DRIVER={{ODBC Driver 18 for SQL Server}};'
            f'SERVER={config.AZURE_SQL_SERVER};'
            f'DATABASE={config.AZURE_SQL_DATABASE};'
            f'UID={config.AZURE_SQL_USERNAME};'
            f'PWD={config.AZURE_SQL_PASSWORD}'
        )
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                query = """
                INSERT INTO HFTPositionHistory (bot_id, mark_price, position_size, timestamp)
                VALUES (?, ?, ?, GETDATE())
                """
                cursor.execute(query, (bot_id, mark_price, position_size))
                conn.commit()
            print(f"Inserted data for {bot_id} into Azure SQL Database")
        except Exception as e:
            print(f"Error inserting data into Azure SQL Database: {str(e)}")

    def get_all_bot_ids(self):
        all_keys = self.redis_client.keys('bot_params_*')
        return [key.decode('utf-8').split('_')[-1] for key in all_keys]

    def get_Gate_Binance_notional(self):
        """
        Pull the data from GB trading bot, get the abs position size and return it.
        """
        position_size = 0
        all_keys = self.redis_client.keys('GB_bot_params_*')
        GB_coin_list = [key.decode('utf-8').split('_')[-1] for key in all_keys]
        for key in GB_coin_list:
            coin_data = self.redis_client.get('GB_bot_params_'+key)
            if coin_data is not None:
                json_string = coin_data.decode('utf-8')
                data = json.loads(json_string)
                position_size += abs(data['position_size'])
        return position_size

    def update_market_data(self, bot_id, mark_price, position_size):
        params_key = f'bot_params_{bot_id}'
        current_params = self.redis_client.get(params_key)
        if current_params:
            current_params = json.loads(current_params)
        else:
            current_params = {}
        current_params.update({
            'mark_price': mark_price,
            'position_size': position_size
        })
        self.redis_client.set(params_key, json.dumps(current_params))
        print(f"Updated data for bot {bot_id}: Mark Price: {mark_price}, Position Size: {position_size}")

    def calculate_total_position(self):
        total_position = 0
        for bot_id in self.get_all_bot_ids():
            params_key = f'bot_params_{bot_id}'
            current_params = self.redis_client.get(params_key)
            if current_params:
                current_params = json.loads(current_params)
                position_size = abs(current_params.get('position_size', 0))
                total_position += position_size
        return total_position

    def adjust_max_notional(self, total_position):
        for bot_id in self.get_all_bot_ids():
            params_key = f'bot_params_{bot_id}'
            current_params = self.redis_client.get(params_key)
            if current_params:
                current_params = json.loads(current_params)
                current_position = abs(current_params.get('position_size', 0))
                default_max_notional = current_params.get('default_max_notional', self.default_max_notional)

                if total_position + self.get_Gate_Binance_notional() >= self.total_position_threshold:
                    new_max_notional = current_position * 0.95
                elif total_position + self.get_Gate_Binance_notional() <= self.reset_threshold:
                    new_max_notional = default_max_notional
                else:
                    continue  # No change needed

                current_params['max_notional'] = new_max_notional
                self.redis_client.set(params_key, json.dumps(current_params))
                print(f"Adjusted max_notional for bot {bot_id} to {new_max_notional}")

    def update_all_bots_market_data(self):
        # Fetch all positions at once
        all_positions = self.exchange.fetch_positions()

        # Create a dictionary for quick access to position data
        position_dict = {pos['symbol']: pos for pos in all_positions}

        for bot_id in self.get_all_bot_ids():
            try:
                # Get position data from the dictionary using the correct symbol format
                position = position_dict.get(f'{bot_id}/USDT:USDT')
                if position is not None:
                    mark_price = float(position['markPrice'])
                    notional_usd = float(position['notional'])

                    # Determine position size (positive for long, negative for short)
                    position_size = notional_usd if position['side'] == 'long' else -notional_usd

                    self.update_market_data(bot_id, mark_price, position_size)
                    self.insert_into_azure_sql(bot_id, mark_price, position_size)
                else:
                    ticker = self.exchange.fetch_ticker(f'{bot_id}/USDT:USDT')
                    mark_price = ticker['last']
                    position_size = 0
                    self.update_market_data(bot_id, mark_price, position_size)
                    self.insert_into_azure_sql(bot_id, mark_price, position_size)

            except Exception as e:
                logger.info(f"Couldn't update market data for {bot_id}: {str(e)}")

        # Calculate total position and adjust max_notional if needed
        total_position = self.calculate_total_position()
        print(f"Total position across all bots: {total_position} + {self.get_Gate_Binance_notional()} = {total_position + self.get_Gate_Binance_notional()} | Max Position : {self.total_position_threshold}")
        self.adjust_max_notional(total_position)


def run_periodic_update():
    try:
        updater = MarketDataUpdater()
        updater.update_all_bots_market_data()
    except Exception as e:
        logger.error(f"Error in run_periodic_update: {str(e)}")
        print("Traceback:")
        traceback.print_exc()
        logger.error("Restarting...")
        time.sleep(5)  # Wait for 5 seconds before restarting
        restart_script()


def restart_script():
    python = sys.executable
    os.execl(python, python, *sys.argv)


if __name__ == "__main__":
    while True:
        try:
            # Run the update immediately when the script starts
            run_periodic_update()

            # Schedule the update to run every minute
            schedule.every(60).seconds.do(run_periodic_update)

            print("Starting periodic market data updates...")
            while True:
                schedule.run_pending()
                time.sleep(1)
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            print("Traceback:")
            traceback.print_exc()
            logger.error("Restarting...")
            time.sleep(5)  # Wait for 5 seconds before restarting
            restart_script()