import sys
import asyncio
import json
import ccxt.async_support as ccxt_async
import redis
import time
import logging
import random
import string
import dummy_config as config
import csv
import os
from datetime import datetime, timedelta
import pytz
import math

"""
Updates:
Add in a dynamic position skew.
Use this position skew to shift band ranges as inventory builds up, shift the bands in a way where loading up becomes harder but exiting becomes easier
as the bot enters more and more positions.

Change the amend order feature. Only amend an order if it is in the trading range. If the spread isn't in trading range, remove the order.
"""

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

#region Initialization
logger = config.setup_logger('HighFrequencyBotV3')
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Initialize bybit and OKX clients
okx = ccxt_async.okx({
    'apiKey': config.OKX_API_KEY,
    'secret': config.OKX_SECRET_KEY,
    'password': config.OKX_PASSPHRASE,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap'
    }
})

bybit = ccxt_async.bybit({
    'apiKey': config.BYBIT_API_KEY,
    'secret': config.BYBIT_SECRET_KEY,
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

#endregion

class CSVLogger:
    def __init__(self, bot_id, max_rows=1000000):
        self.bot_id = bot_id
        self.max_rows = max_rows
        self.current_file = None
        self.current_writer = None
        self.row_count = 0
        self.log_attempt_count = 0
        self.create_new_file()

    def create_new_file(self):
        if self.current_file:
            self.current_file.close()
        timestamp = datetime.now().strftime("%Y%m%d")
        onedrive_path = "tradelogs/"
        filename = os.path.join(onedrive_path, f"bot_{self.bot_id}_{timestamp}_V3.5.csv")
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        file_exists = os.path.isfile(filename)
        self.current_file = open(filename, 'a', newline='')  # 'a' for append mode
        self.current_writer = csv.writer(self.current_file)

        # Get the current row count
        self.current_file.seek(0, os.SEEK_END)
        self.row_count = self.current_file.tell() // 100

    def log(self, side, ma_entry_spread, entry_spread, ma_exit_spread, exit_spread, limit_order, fr_factor, entry_bound, exit_bound, impact_bid_price_okx, impact_ask_price_bybit, buy_spread_ma, sell_spread_ma, buy_spread_sd, sell_spead_sd, timeoftrade, okx_orderbook, bybit_orderbook, impact_price_flag):
        self.log_attempt_count += 1  # Increment the counter

        # Only log if it's the 3rd attempt (or a multiple of 3)
        if self.log_attempt_count % 3 == 0:
            if self.row_count >= self.max_rows:
                self.create_new_file()

            row = [
                timeoftrade,
                self.bot_id,
                side,
                round(ma_entry_spread, 3),
                round(entry_spread, 3),
                round(ma_exit_spread, 3),
                round(exit_spread, 3),
                round(limit_order, 6),
                fr_factor,
                round(entry_bound, 5),
                round(exit_bound, 5),
                impact_bid_price_okx,
                impact_ask_price_bybit,
                buy_spread_ma,
                sell_spread_ma,
                buy_spread_sd,
                sell_spead_sd,
                okx_orderbook,
                bybit_orderbook,
                impact_price_flag
            ]
            self.current_writer.writerow(row)
            self.row_count += 1
            self.current_file.flush()  # Ensure data is written immediately

    def close(self):
        if self.current_file:
            self.current_file.close()


class BotIdFilter(logging.Filter):
    def __init__(self, bot_id):
        super().__init__()
        self.bot_id = bot_id

    def filter(self, record):
        record.bot_id = self.bot_id
        return True


class TradingBot:
    def __init__(self, symbol, notional_per_trade, bot_id, max_notional):
        self.csv_logger = CSVLogger(bot_id)
        self.symbol = symbol
        # self.params_key = f'bot_params_{bot_id}'
        self.params_key = f'Perp_Perp_bot_params_{bot_id}'
        self.default_params = {
            'okx_contracts_per_trade': 1,
            'ma': 10,
            'max_notional': max_notional,
            'max_positions': 1,
            'notional_per_trade': notional_per_trade,
            'std_coeff': 1,
            'min_width': 0.07,
            'max_skew': 0.02
        }
        self.params = self.default_params.copy()
        self.max_positions = None
        self.in_trade_range = False
        self.okx_contracts_per_trade = None
        self.entry_bound = None
        self.exit_bound = None
        self.current_okx_order_id = None
        self.previous_entry_spread = None
        self.previous_exit_spread = None
        self.previous_side = None
        self.adjustment_value = None
        self.contract_size = None
        self.mark_price = None
        self.contract_size = None
        self.bot_id = bot_id
        self.stop_trading_key = f'stop_trading_{symbol}'
        self.notional_per_trade = notional_per_trade
        self.max_notional = max_notional
        self.last_redis_position_size = None
        self.position_change_since_update = 0
        self.update_params()
        self.timeoftrade = None
        bot_id_filter = BotIdFilter(self.bot_id)
        logger.addFilter(bot_id_filter)
        logger.info(f'Bot ID: {bot_id} | Max Notional: {self.max_notional} | Notional per Trade: {self.notional_per_trade} / {self.okx_contracts_per_trade} contracts | Percent to max notional: {round(100*self.position_size / self.max_notional, 3)}%')

    def update_params(self):
        params_json = redis_client.get(self.params_key)
        sd_adjustment = redis_client.get('sd_adjustment')
        min_width_adjustment = redis_client.get('min_width_adjustment')

        if params_json:
            updated_params = json.loads(params_json)
            self.params.update(updated_params)

        # Update instance variables
        self.contract_size = config.OKX_CONTRACT_SZ.get(f'{self.symbol}-USDT-SWAP')
        self.notional_per_trade = self.params['notional_per_trade']
        self.max_notional = self.params['max_notional']
        self.ma = self.params['ma']
        self.max_skew = self.params['max_skew']
        self.std_coeff = self.params['std_coeff'] # * abs(float(sd_adjustment)) These two values are used from the risk management bot which is on pause for now
        self.min_width = self.params['min_width'] # * abs(float(min_width_adjustment))
        self.mark_price = self.params['mark_price']
        self.position_size = self.params['position_size']

        # Only update okx_contracts_per_trade if mark_price and contract_size are set
        if self.mark_price is not None:
            self.okx_contracts_per_trade = round(self.notional_per_trade / (self.mark_price * self.contract_size))
        else:
            self.okx_contracts_per_trade = 0
            logger.info(f"Warning: mark_price or contract_size not set for {self.symbol}. Cannot update okx_contracts_per_trade.")

        if 'position_size' in self.params:
            new_position_size = self.params['position_size']
            if self.last_redis_position_size != new_position_size:
                # A new upload has occurred
                self.last_redis_position_size = new_position_size
                self.position_change_since_update = 0

    def get_current_position_size(self):
        return self.last_redis_position_size + self.position_change_since_update

    @staticmethod
    def generate_client_order_id():
        """Generate a simple alphanumeric client order ID."""
        return 'PerpPerpArb' + ''.join(random.choices(string.ascii_letters + string.digits, k=10))

    @staticmethod
    def get_current_time_ms():
        return int(time.time() * 1000)

    async def initialize_clients(self):
        print('initializing markets')
        await bybit.load_markets()
        await okx.load_markets()
        try:
            await okx.set_leverage(8, f'{self.symbol}-USDT-SWAP')
            await bybit.set_leverage(8, f'{self.symbol}USDT')
        except Exception as e:
            if 'leverage not modified' in str(e).lower():
                logger.info(f"Leverage already set to 8")
            else:
                logger.error(f"Error setting leverage: {e}")

    @staticmethod
    def get_latest_data(key, count):
        data = redis_client.lrange(key, -count, -1)
        return [json.loads(item) for item in data]

    @staticmethod
    def is_paused():
        return redis_client.get('server_overload_pause') is not None

    @staticmethod
    def get_latest_row(key):
        data = redis_client.lrange(key, -1, -1)
        return json.loads(data[0]) if data else None

    @staticmethod
    def calculate_ma(data, key):
        if not data:
            return None

        # Use Hong Kong timezone
        current_time = datetime.now()
        one_second_ago = current_time - timedelta(seconds=1)

        # Filter data points from the last second
        recent_values = []
        for item in data:
            try:
                # Parse the timestamp into datetime.datetime object
                item_time = datetime.fromtimestamp(item['timestamp'] / 1000)
                if item_time > one_second_ago and item[key] is not None:  # Check if the value is not None
                    recent_values.append(item[key])
            except ValueError:
                # If timestamp parsing fails, skip this item
                continue

        if recent_values:
            # Return average of values from the last second
            return sum(recent_values) / len(recent_values)
        elif data:
            # If no recent values, check the key for "entry" or "exit". Return -10 and 10 since these will force the bot to increase buffer.
            if "entry" in key.lower():
                return -10
            elif "exit" in key.lower():
                return 10
            else:
                # If neither "entry" nor "exit" is in the key, return the last non-None value
                for item in reversed(data):
                    if item[key] is not None:
                        return item[key]

        # If we've reached this point, there's no data at all
        return None

    @staticmethod
    def calculate_sd(data, key, ma):
        values = [item[key] for item in data]
        if not values or ma is None:
            return 0
        variance = sum((x - ma) ** 2 for x in values) / len(values)
        return variance ** 0.5 if variance > 0 else None

    @staticmethod
    def should_continue_trading():
        stop_flag = redis_client.get('stop_bot')
        return stop_flag != b'true'

    def get_trend_data(self, coin, fr_adjustment_factor):
        """
        Get the trend data from TrendsRedisUpload.py
        Get the inventory data from PositionUpload.py
        Use the position data to in
        """
        # Fetch Position Data
        position_data = redis_client.get(f'Perp_Perp_bot_params_{coin}')
        position_data_dict = json.loads(position_data)
        current_position = float(position_data_dict['position_size'])
        max_position = 300000   # Max position used to calculate the current position ratio
        capacity = round(current_position / max_position, 3)  # This naturally gives us [-1, 1]

        skew = self.calculate_skew(capacity, self.max_skew)

        data = redis_client.hget('trend_data', f'{coin}/USDT')

        if data is None:
            # If the data is none (if the data is pulled during the flush, then return a spread of 1% on both ends)
            return 1, -1, None
        coin_data = json.loads(data)

        sell_bound = max((coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M'])/2 + coin_data['sell_spread_sd_L']*self.std_coeff, self.min_width/2 + (coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M'])/2) + max(fr_adjustment_factor, 0) + skew

        buy_bound = min((coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M'])/2 - coin_data['buy_spread_sd_L']*self.std_coeff, (coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M'])/2 - self.min_width/2) + min(fr_adjustment_factor, 0) + skew

        return sell_bound, buy_bound, coin_data, coin_data['buy_spread_ma_M'], coin_data['sell_spread_ma_M'], coin_data['buy_spread_sd_L'], coin_data['sell_spread_sd_L'], skew

    async def place_okx_limit_order(self, symbol, side, quantity, price):
        # Add in the client ID for the specific strategy
        try:
            client_order_id = str(self.generate_client_order_id())
            order = await okx.create_order(symbol, "post_only", side, quantity, price,
                                           params={'clientOrderId': client_order_id})
            return order['id']
        except Exception as e:
            logger.info(f"Failed to place OKX limit order: {e}")
            return None

    async def edit_okx_limit_order(self, order_id, symbol, side, quantity, price):
        order = await okx.edit_order(order_id, symbol, 'post_only', side, quantity, price)
        return order['id']

    async def cancel_okx_order(self, order_id, symbol):
        result = await okx.cancel_order(order_id, symbol)
        print(f"Cancelled order {order_id} for {symbol}")
        return result

    def calculate_skew(self, capacity, max_skew=0.1):
        """
        Calculate position skew with exponential increase near edges
        """
        # Square function (x²) - moderate edge severity
        return -math.copysign(1, capacity) * (capacity * capacity) * max_skew

    def log_order_action(self, side, ma_entry_spread, ma_exit_spread, latest_row, entry_limit_price,
                         exit_limit_price, fr_adjustment_factor, buy_spread_ma, sell_spread_ma, buy_spread_sd, sell_spead_sd, skew):
        self.csv_logger.log(
            side,
            ma_entry_spread,
            latest_row["entry_spread"],
            ma_exit_spread,
            latest_row["exit_spread"],
            entry_limit_price if side == 'sell' else exit_limit_price,
            fr_adjustment_factor,
            self.entry_bound,
            self.exit_bound,
            latest_row["impact_bid_price_okx"],
            latest_row["impact_ask_price_bybit"],
            buy_spread_ma,
            sell_spread_ma,
            buy_spread_sd,
            sell_spead_sd,
            self.timeoftrade,
            latest_row['okx_orderbook'],
            latest_row['bybit_orderbook'],
            latest_row['impact_price_reached'],
        )
        if side == 'sell':
            print(
                f'({self.in_trade_range} - {datetime.now()}) | Bot {self.bot_id} | {side} order | sell ma: {round(ma_entry_spread, 3)} | sell spread: {round(latest_row["entry_spread"], 3)} | limit order: {round(entry_limit_price, 6)} | fr factor: {fr_adjustment_factor} | bollinger bands: ({round(self.entry_bound, 5)}, {round(self.exit_bound, 5)}) | price (O/B): ({latest_row["impact_bid_price_okx"]}/{latest_row["impact_ask_price_bybit"]}) | Skew: {skew}')
        elif side == 'buy':
            print(
                f'({self.in_trade_range} - {datetime.now()}) | Bot {self.bot_id} | {side} order | buy ma: {round(ma_exit_spread, 3)} | buy spread: {round(latest_row["exit_spread"], 3)} | limit order: {round(exit_limit_price, 6)} | fr factor: {fr_adjustment_factor} | bollinger bands: ({round(self.entry_bound, 5)}, {round(self.exit_bound, 5)}) | price (O/B): ({latest_row["impact_ask_price_okx"]}/{latest_row["impact_bid_price_bybit"]} | Skew: {skew})')

    def choose_direction(self, current_entry_spread, current_exit_spread):
        """
        Determine the direction for the trade based on position limits and spread distances.
        """
        # Check if we've reached the maximum notional value
        if abs(self.get_current_position_size()) + self.notional_per_trade > self.max_notional:
            return 'sell' if self.get_current_position_size() > 0 else 'buy'    # Changed this to be self.get_current_position_size instead of self.position_size which could be lagging.
        # If neither limit is reached, decide based on spread distances
        entry_distance = self.entry_bound - current_entry_spread
        exit_distance = current_exit_spread - self.exit_bound
        if self.max_notional == 0 and self.notional_per_trade == 0:
            return None
        return 'sell' if entry_distance < exit_distance else 'buy'

    async def stop_trading(self):
        logger.info(f"Bot {self.bot_id} stopping trading operations.")
        config.send_telegram_message(
            f"Stop trading flag triggered, {self.bot_id} is shutting down and cancelling all open orders... please check High Frequency Bot",
            config.bot_token2, config.chat_id2)
        # Cancel any open orders
        if self.current_okx_order_id:
            try:
                await self.cancel_okx_order(self.current_okx_order_id, f'{self.symbol}-USDT-SWAP')
            except Exception as e:
                logger.error(f"Error cancelling order during stop: {e}")

    def calculate_spreads_and_limits(self, latest_data, latest_row):
        """
        Calculate the necessary spreads and limit prices based on the latest data and row,
        and include the funding rate adjustment factor in the calculations.
        """
        ma_entry_spread = self.calculate_ma(latest_data, 'entry_spread')
        ma_exit_spread = self.calculate_ma(latest_data, 'exit_spread')
        current_entry_spread = latest_row['entry_spread']
        current_exit_spread = latest_row['exit_spread']
        entry_limit_price = latest_row['best_ask_price_okx']
        exit_limit_price = latest_row['best_bid_price_okx']

        if ma_entry_spread is None or ma_exit_spread is None or current_entry_spread is None or current_exit_spread is None:
            print(f"Missing spread data for {self.symbol}, either websocket is closed for this symbol or Impact hasn't been reached")
            return None
        if self.is_paused():
            logger.info(f"Bot {self.bot_id} temporarily adjusting limits due to recent server overload.")
            entry_limit_price += self.adjustment_value
            exit_limit_price -= self.adjustment_value
        else:
            if ma_entry_spread < self.entry_bound or latest_row['entry_spread'] < self.entry_bound:
                entry_limit_price += self.adjustment_value
            if ma_exit_spread > self.exit_bound or latest_row['exit_spread'] > self.exit_bound:
                exit_limit_price -= self.adjustment_value
        return {
            "ma_entry_spread": ma_entry_spread,
            "ma_exit_spread": ma_exit_spread,
            "current_entry_spread": current_entry_spread,
            "current_exit_spread": current_exit_spread,
            "entry_limit_price": entry_limit_price,
            "exit_limit_price": exit_limit_price
        }

    async def main(self):
        print('starting...')
        await self.initialize_clients()
        # initially pull the latest row once to get a 25% adjustment buffer value
        self.adjustment_value = (self.get_latest_row(f'OKX_PERP_BYBIT_PERP_{self.symbol}'))['best_bid_price_okx'] * 0.25
        while True:
            self.in_trade_range = False
            self.timeoftrade = datetime.now()
            if not self.should_continue_trading():
                logger.info(f"Bot {self.bot_id} stopping due to stop_run_bot flag.")
                await self.stop_trading()
                break
            self.update_params()
            latest_data = self.get_latest_data(f'OKX_PERP_BYBIT_PERP_{self.symbol}', count=self.ma)
            if latest_data and len(latest_data) > 0:
                latest_row = latest_data[-1]
            else:
                logger.warning(f"No data available for {self.symbol}")
                continue

            # Check if current time is within the first 5 minutes of a 4-hour interval for FR adjustment factor
            current_time = datetime.now()
            if current_time.hour % 4 == 0 and current_time.minute < 5:
                fr_adjustment_factor = 0
            else:
                # Fetch the funding rate adjustment factor from Redis for the specific coin
                fr_adjustment_factor = 0
                json_data = redis_client.get(f'funding_rates:{self.symbol}')
                if json_data:
                    data = json.loads(json_data)
                    fr_adjustment_factor = data.get('fr_adjustment_factor', 0)

            self.entry_bound, self.exit_bound, coin_data, buy_spread_ma, sell_spread_ma, buy_spread_sd, sell_spead_sd, skew = self.get_trend_data(self.symbol, fr_adjustment_factor)

            if latest_data and len(latest_data) >= self.ma:
                spreads_and_limits = self.calculate_spreads_and_limits(latest_data, latest_row)
                if spreads_and_limits is None:
                    print(
                        f"Unable to calculate spreads and limits for {self.symbol}. Skipping this iteration.")
                    await asyncio.sleep(0.1)
                    continue
                ma_entry_spread = spreads_and_limits["ma_entry_spread"]
                ma_exit_spread = spreads_and_limits["ma_exit_spread"]
                current_entry_spread = spreads_and_limits["current_entry_spread"]
                current_exit_spread = spreads_and_limits["current_exit_spread"]
                entry_limit_price = spreads_and_limits["entry_limit_price"]
                exit_limit_price = spreads_and_limits["exit_limit_price"]
                side = self.choose_direction(current_entry_spread, current_exit_spread)

                if side == 'buy':
                    limit_price = exit_limit_price
                    if limit_price == latest_row["best_bid_price_okx"]:
                        self.in_trade_range = True
                else:
                    limit_price = entry_limit_price
                    if limit_price == latest_row["best_ask_price_okx"]:
                        self.in_trade_range = True

                if side is None:
                    logger.info(f"Bot {self.bot_id} has no trade opportunity at this time.")
                    await asyncio.sleep(0.02)
                    continue

                try:
                    self.log_order_action(side, ma_entry_spread, ma_exit_spread, latest_row, entry_limit_price,
                                          exit_limit_price, fr_adjustment_factor, buy_spread_ma, sell_spread_ma,
                                          buy_spread_sd, sell_spead_sd, skew)
                    # Check if in trade range.
                    # If in range and current_okx_order_id is None, then place a new order
                    # If in range and current_okx_order_id has a value, then amend that order if needed.
                    # If not in range and current_okx_order_id is None, then skip
                    # If not in range and current_okx_order_id has a value, then cancel the order
                    if self.in_trade_range:
                        print(f'in Trade Range - ID {self.current_okx_order_id}')
                        if self.current_okx_order_id is None:
                            self.current_okx_order_id = await self.place_okx_limit_order(f'{self.symbol}-USDT-SWAP', side,
                                                                                         self.okx_contracts_per_trade,
                                                                                         limit_price)
                            print(f"Order placed: {self.current_okx_order_id}")
                        else:
                            # Code below for modifying/cancelling the existing order and aligning them with the current data
                            self.current_okx_order_id = await self.edit_okx_limit_order(self.current_okx_order_id,
                                                                                            f'{self.symbol}-USDT-SWAP',
                                                                                            side,
                                                                                            self.okx_contracts_per_trade,
                                                                                            limit_price)
                    else:
                        if self.current_okx_order_id is None:
                            pass
                        else:
                            self.current_okx_order_id = await self.cancel_okx_order(self.current_okx_order_id,
                                                                                        f'{self.symbol}-USDT-SWAP')
                            self.current_okx_order_id = None
                except Exception as e:
                    if any(msg in str(e) for msg in ['Your order has already been filled or canceled',
                                                     'Order cancellation failed as the order has been filled, canceled or does not exist']):
                        self.current_okx_order_id = None
                        if self.previous_side == 'buy':
                            self.position_change_since_update += self.notional_per_trade
                        elif self.previous_side == 'sell':
                            self.position_change_since_update -= self.notional_per_trade

                        current_position = self.get_current_position_size()
                        abs_current_position = abs(current_position)

                        logger.info(
                            f'Order filled | {self.symbol} | {self.previous_side} | Notional: {self.notional_per_trade} | '
                            f'Position: {round(current_position, 2)} | Abs Position: {round(abs_current_position, 2)} | '
                            f'Max Notional: {self.max_notional} | Change Since Update: {round(self.position_change_since_update, 2)} | '
                            f'Time of Trade: {self.timeoftrade} | Current Time: {datetime.now()} | '
                            f'latency: {(datetime.now() - self.timeoftrade).total_seconds() * 1000}'
                        )

                        print(
                            f'Order filled | {self.symbol} | Current position: {abs_current_position}/{self.max_notional}')
                        await asyncio.sleep(0.025)
                    elif 'Number of modification requests that are currently in progress for an order cannot exceed 3 times' in str(e):
                        logger.info(f'Modification Requests for exceeded limit for {self.symbol}, pausing for 0.5 seconds')
                        await asyncio.sleep(0.5)
                    elif 'Systems are busy. Please try again later.' in str(e):
                        logger.info(f'Systems busy for {self.symbol}, pausing for 0.5 seconds - (OKX Server overload issue)')
                        await asyncio.sleep(0.5)
                    elif 'Service temporarily unavailable. Please try again later' in str(e):
                        logger.info(f'Systems busy for {self.symbol}, pausing for 0.5 seconds - (OKX Server overload issue)')
                        await asyncio.sleep(0.5)
                    elif 'notional must be no smaller than' in str(e):
                        logger.info(f'Notional too small for {self.symbol}, placing into unhedged...')
                        await asyncio.sleep(0.5)
                    elif '[Errno 22] Invalid argument' in str(e):
                        logger.error(f"Invalid argument error for {self.bot_id}: {e}")
                        logger.error(
                            f"Current state - Side: {side}, Limit Price: {limit_price}, Contracts: {self.okx_contracts_per_trade}")
                    else:
                        logger.error(f'Unexpected error while managing order for {self.bot_id}: {e}')
                        logger.error(
                            f"Current state - Side: {side}, Limit Price: {limit_price}, Contracts: {self.okx_contracts_per_trade}")

                self.previous_entry_spread = current_entry_spread
                self.previous_exit_spread = current_exit_spread
                self.previous_side = side
            await asyncio.sleep(0.025)