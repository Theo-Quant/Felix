import asyncio
import json
import ccxt.async_support as ccxt_async
import redis
import time
import logging
import random
import string
# import config
import TradingModule_PPConfig as config
import csv
import os
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv

#region Initialization
logger = config.setup_logger('PerpPerpManualBot')
redis_client = redis.Redis(host='localhost', port=6379, db=0)

load_dotenv()
OKX_API_KEY = os.getenv('OKX_API_KEY')
OKX_SECRET_KEY = os.getenv('OKX_SECRET_KEY')
OKX_PASSPHRASE = os.getenv('OKX_PASSPHRASE')

BYBIT_API_KEY = os.getenv('BYBIT_API_KEY')
BYBIT_SECRET_KEY = os.getenv('BYBIT_SECRET_KEY')

onedrive_path = os.getenv('onedrive_path')

# Initialize Binance and OKX clients
binance = ccxt_async.binance({
    'apiKey': config.BINANCE_API_KEY,
    'secret': config.BINANCE_SECRET_KEY,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future'
    }
})

okx = ccxt_async.okx({
    'apiKey': OKX_API_KEY,
    'secret': OKX_SECRET_KEY,
    'password': OKX_PASSPHRASE,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap'
    }
})

bybit = ccxt_async.bybit({
    'apiKey': BYBIT_API_KEY,
    'secret': BYBIT_SECRET_KEY,
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
        self.create_new_file()

    def create_new_file(self):
        if self.current_file:
            self.current_file.close()
        timestamp = datetime.now().strftime("%Y%m%d")
        filename = os.path.join(onedrive_path, f"bot_{self.bot_id}_PP_{timestamp}_V3.5.csv")
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        file_exists = os.path.isfile(filename)
        self.current_file = open(filename, 'a', newline='')  # 'a' for append mode
        self.current_writer = csv.writer(self.current_file)

        # Get the current row count
        self.current_file.seek(0, os.SEEK_END)
        self.row_count = self.current_file.tell() // 100

    def log(self, side, ma_entry_spread, entry_spread, ma_exit_spread, exit_spread, limit_order, fr_factor, entry_bound, exit_bound, buy_spread_ma, sell_spread_ma, buy_spread_sd, sell_spead_sd):
        if self.row_count >= self.max_rows:
            self.create_new_file()

        row = [
            datetime.now(),
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
            buy_spread_ma,
            sell_spread_ma,
            buy_spread_sd,
            sell_spead_sd
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
    def __init__(self, exchange_perp1, exchange_perp2, symbol, notional_per_trade, bot_id, max_notional, entry_spread_threshold, exit_spread_threshold):
        self.csv_logger = CSVLogger(bot_id)
        self.exchange_perp1 = exchange_perp1
        self.exchange_perp2 = exchange_perp2
        self.symbol = symbol
        self.params_key = f'Perp_Perp_bot_params_{bot_id}'
        self.default_params = {
            'contracts_per_trade': 1,
            'ma': 100,
            'max_notional': max_notional,
            'max_positions': 1,
            'notional_per_trade': notional_per_trade,
            'std_coeff': 1,
            'min_width': 0.07
        }
        self.params = self.default_params.copy()
        self.max_positions = None
        self.contracts_per_trade = None
        self.entry_bound = None
        self.exit_bound = None
        self.previous_entry_spread = None
        self.previous_exit_spread = None
        self.previous_side = None
        self.adjustment_value = None
        self.contract_size = None
        self.mark_price = None
        self.current_entry_order_id = None
        self.current_exit_order_id = None
        self.bot_id = bot_id
        self.stop_trading_key = f'stop_trading_{symbol}'
        self.notional_per_trade = notional_per_trade
        self.max_notional = max_notional
        self.last_redis_position_size = None
        self.position_change_since_update = 0
        self.update_params()
        self.entry_spread_threshold = entry_spread_threshold
        self.exit_spread_threshold = exit_spread_threshold
        bot_id_filter = BotIdFilter(self.bot_id)
        logger.addFilter(bot_id_filter)
        logger.info(
            f'Bot ID: {bot_id} | Exchange: {self.exchange_perp1} | Max Notional: {self.max_notional} | Notional per Trade: {self.notional_per_trade} / {self.contracts_per_trade} contracts | Percent to max notional: {round(100 * self.position_size / self.max_notional, 3)}%')

    def update_params(self):
        params_json = redis_client.get(self.params_key)
        if params_json:
            updated_params = json.loads(params_json)
            self.params.update(updated_params)

        # Update instance variables
        if self.exchange_perp1 == 'okx':
            self.contract_size = config.OKX_CONTRACT_SZ.get(f'{self.symbol}-USDT-SWAP', 1)
        elif self.exchange_perp1 == 'bybit':
            self.contract_size = 1
        self.notional_per_trade = self.params['notional_per_trade']
        self.max_notional = self.params['max_notional']
        self.ma = self.params['ma']
        self.std_coeff = self.params['std_coeff']
        self.min_width = self.params['min_width']

        # Comes from PositionUpload_PP.py
        if self.params['okx_mark_price'] != 0:
            self.mark_price = self.params['okx_mark_price']
        elif self.params['bybit_mark_price'] != 0:
            self.mark_price = self.params['bybit_mark_price']
        # else:
        #     self.mark_price = self.params['gate_mark_price']
        print(self.mark_price)

        self.position_size = self.params['total_position_size']

        # Update contracts_per_trade
        if self.mark_price is not None:
            if self.exchange_perp1 == 'okx':
                self.contracts_per_trade = round(self.notional_per_trade / (float(self.mark_price) * self.contract_size))
            else:
                self.contracts_per_trade = round(self.notional_per_trade / (float(self.mark_price)))
        else:
            self.contracts_per_trade = 0
            print("Warning: mark_price or contract_size not set. Cannot update contracts_per_trade.")

        if 'total_position_size' in self.params:
            new_position_size = self.params['total_position_size']
            if self.last_redis_position_size != new_position_size:
                self.last_redis_position_size = new_position_size
                self.position_change_since_update = 0

    def get_current_position_size(self):
        return self.last_redis_position_size + self.position_change_since_update

    async def initialize_clients(self):
        print('initializing markets')
        await okx.load_markets()
        await bybit.load_markets()
        try:
            await okx.set_leverage(8, f'{self.symbol}-USDT-SWAP')
            await bybit.set_leverage(8, f'{self.symbol}USDT')
        except Exception as e:
            if 'leverage not modified' in str(e).lower():
                logger.info(f"Leverage already set to 8")
            else:
                logger.error(f"Error setting leverage: {e}")

    @staticmethod
    def generate_client_order_id():
        return 'PerpPerpArb' + ''.join(random.choices(string.ascii_letters + string.digits, k=10))

    @staticmethod
    def get_current_time_ms():
        return int(time.time() * 1000)

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
        hk_tz = pytz.timezone('Asia/Hong_Kong')
        current_time = datetime.now(hk_tz)
        one_second_ago = current_time - timedelta(seconds=1)

        # Filter data points from the last second
        recent_values = []
        for item in data:
            try:
                # Convert the timestamp (assuming it's in milliseconds) to a datetime object
                item_time = datetime.fromtimestamp(item['timestamp'] / 1000, tz=pytz.UTC).astimezone(hk_tz)
                if item_time > one_second_ago and item[key] is not None:  # Check if the value is not None
                    recent_values.append(item[key])
            except (ValueError, TypeError, KeyError):
                # If timestamp conversion fails, skip this item
                continue

        if len(recent_values) > 0:
            # Return average of values from the last second
            return sum(recent_values) / len(recent_values)
        elif data:
            # If no recent values, return the most recent non-None value
            for item in reversed(data):
                if item[key] is not None:
                    return item[key]
        # If all values are None, return None
        return None

    def get_trend_data(self, coin, fr_adjustment_factor):
        # entry bound, exit bound, coin_data, buy_spread_ma, sell_spread_ma, buy_spread_sd, sell_spead_sd
        return self.entry_spread_threshold, self.exit_spread_threshold, None, 0, 0, 0, 0

    async def place_limit_order(self, symbol, side, quantity, price):
        try:
            client_order_id = self.generate_client_order_id()
            if self.exchange_perp1 == 'okx':
                # quantity = round(quantity, 0)
                order = await okx.create_order(symbol, "post_only", side, quantity, price,
                                               params={'clientOrderId': client_order_id})
                return order['id']
            elif self.exchange_perp1 == 'binance':
                order = await binance.create_order(symbol, "limit", side, quantity, price,
                                                   params={'newClientOrderId': client_order_id, 'postOnly': True})
                print('Binance Order', order)
                return order['id']  # Ensure we're returning the 'id' field for Binance as well
            elif self.exchange_perp1 == 'bybit':
                order = await bybit.create_order(symbol, "limit", side, quantity, price,
                                                    params={'order_link_id': client_order_id, 
                                                            'postOnly': True})
                return order['info']['orderId']
        except Exception as e:
            print(f"Failed to place {self.exchange_perp1} limit order: {e}")
            return None

    async def edit_limit_order(self, order_id, symbol, side, quantity, price):
        if self.exchange_perp1 == 'okx':
            order = await okx.edit_order(order_id, symbol, 'post_only', side, quantity, price)
            return order['id']
        elif self.exchange_perp1 == 'binance':
            await self.cancel_order(order_id, symbol)
            new_order_id = await self.place_limit_order(symbol, side, quantity, price)
            return new_order_id
        elif self.exchange_perp1 == 'bybit':
            order = await bybit.edit_order(order_id, symbol, 'limit', side, quantity, price, params={'postOnly': True})
            return order['info']['result']['orderId']

    async def cancel_order(self, order_id, symbol):
        try:
            if self.exchange_perp1 == 'okx':
                result = await okx.cancel_order(order_id, symbol)
            elif self.exchange_perp1 == 'binance':
                result = await binance.cancel_order(order_id, symbol)
            elif self.exchange_perp1 == 'bybit':
                result = await bybit.cancel_order(order_id, symbol)
            print(f"Cancelled order {order_id} for {symbol}")
            return result
        except Exception as e:
            print(f"Failed to cancel {self.exchange_perp1} order: {e}")
            return None

    def calculate_spreads_and_limits(self, latest_data, latest_row):
        ma_entry_spread = self.calculate_ma(latest_data, 'entry_spread')
        ma_exit_spread = self.calculate_ma(latest_data, 'exit_spread')
        current_entry_spread = latest_row['entry_spread']
        current_exit_spread = latest_row['exit_spread']
        entry_limit_price = latest_row[f'perp1_best_ask']
        exit_limit_price = latest_row[f'perp1_best_bid']

        if ma_entry_spread is None or ma_exit_spread is None or current_entry_spread is None or current_exit_spread is None:
            print(
                f"Missing spread data for {self.symbol}, either websocket is closed for this symbol or Impact hasn't been reached")
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
            "entry_limit_price": entry_limit_price,     # sell
            "exit_limit_price": exit_limit_price    # buy
        }

    def log_order_action(self, side, ma_entry_spread, ma_exit_spread, latest_row, entry_limit_price,
                         exit_limit_price, fr_adjustment_factor, buy_spread_ma, sell_spread_ma, buy_spread_sd, sell_spead_sd):
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
            buy_spread_ma,
            sell_spread_ma,
            buy_spread_sd,
            sell_spead_sd
        )

        if side == 'sell':
            print(
                f'(Position: ${round(self.get_current_position_size(), 2)} / ${round(self.max_notional, 2)} | {datetime.now()}) | Bot {self.bot_id} | {side} order | sell ma: {round(ma_entry_spread, 3)} | sell spread: {round(latest_row["entry_spread"], 3)} | limit order: {round(entry_limit_price, 6)} | fr factor: {fr_adjustment_factor} | bollinger bands: ({round(self.entry_bound, 5)}, {round(self.exit_bound, 5)})')
        elif side == 'buy':
            print(
                f'(Position: ${round(self.get_current_position_size(), 2)} / ${round(self.max_notional, 2)} | {datetime.now()}) | Bot {self.bot_id} | {side} order | buy ma: {round(ma_exit_spread, 3)} | buy spread: {round(latest_row["exit_spread"], 3)} | limit order: {round(exit_limit_price, 6)} | fr factor: {fr_adjustment_factor} | bollinger bands: ({round(self.entry_bound, 5)}, {round(self.exit_bound, 5)}))')

    async def main(self):
        print('starting...')
        await self.initialize_clients()
        self.adjustment_value = (self.get_latest_row(f"{self.exchange_perp1.upper()}_PERP_{self.exchange_perp2.upper()}_PERP_{self.symbol}"))['perp1_best_bid'] * 0.1

        # This limit price is to track the change of limit price
        original_exit_limit_price = None
        original_entry_limit_price = None

        if self.exchange_perp1 == 'okx':
            trade_symbol = f'{self.symbol}-USDT-SWAP'
        elif self.exchange_perp1 == 'bybit':
            trade_symbol = f'{self.symbol}USDT'
        # else if the exchange is Binance
        # TODO Make sure to change this if we add in more exchanges
        else:
            trade_symbol = f'{self.symbol}/USDT'

        while True:
            latest_data = self.get_latest_data(f"{self.exchange_perp1.upper()}_PERP_{self.exchange_perp2.upper()}_PERP_{self.symbol}", count=self.ma)
            latest_row = self.get_latest_row(f"{self.exchange_perp1.upper()}_PERP_{self.exchange_perp2.upper()}_PERP_{self.symbol}")

            fr_adjustment_factor = 0

            self.entry_bound, self.exit_bound, coin_data, buy_spread_ma, sell_spread_ma, buy_spread_sd, sell_spead_sd = self.get_trend_data(
                self.symbol, fr_adjustment_factor)

            if latest_data and len(latest_data) >= self.ma:
                spreads_and_limits = self.calculate_spreads_and_limits(latest_data, latest_row)
                if spreads_and_limits is None:
                    print(f"Unable to calculate spreads and limits for {self.symbol}. Skipping this iteration.")
                    await asyncio.sleep(0.05)
                    continue

                ma_entry_spread = spreads_and_limits["ma_entry_spread"]
                ma_exit_spread = spreads_and_limits["ma_exit_spread"]
                current_entry_spread = spreads_and_limits["current_entry_spread"]
                current_exit_spread = spreads_and_limits["current_exit_spread"]
                entry_limit_price = spreads_and_limits["entry_limit_price"]
                exit_limit_price = spreads_and_limits["exit_limit_price"]

                # If the limits haven't changed, go to the next iteration
                if original_entry_limit_price == entry_limit_price and original_exit_limit_price == exit_limit_price:
                    await asyncio.sleep(0.05)
                    continue
             
                # Entry
                try:
                    if not(self.position_change_since_update < -self.max_notional):
                        if self.current_entry_order_id is None and entry_limit_price != original_entry_limit_price:
                            self.log_order_action('sell', ma_entry_spread, ma_exit_spread, latest_row, entry_limit_price,
                                                exit_limit_price, fr_adjustment_factor, buy_spread_ma, sell_spread_ma,
                                                buy_spread_sd, sell_spead_sd)
                            self.current_entry_order_id = await self.place_limit_order(trade_symbol, 'sell',
                                                                                self.contracts_per_trade,
                                                                                entry_limit_price)
                            print(f"Order placed: {self.current_entry_order_id}")
                        elif self.current_entry_order_id and entry_limit_price != original_entry_limit_price:
                            self.log_order_action('sell', ma_entry_spread, ma_exit_spread, latest_row, entry_limit_price,
                                                exit_limit_price, fr_adjustment_factor, buy_spread_ma, sell_spread_ma,
                                                buy_spread_sd, sell_spead_sd)

                            self.current_entry_order_id = await self.edit_limit_order(self.current_entry_order_id,
                                                                                    trade_symbol,
                                                                                    'sell',
                                                                                    self.contracts_per_trade,
                                                                                    entry_limit_price)
                        original_entry_limit_price = entry_limit_price
                except Exception as e:
                    if any(msg in str(e) for msg in ['Your order has already been filled or canceled',
                                                     'Order cancellation failed as the order has been filled, canceled or does not exist',
                                                     'order not exists or too late to replace']):
                        
                        self.position_change_since_update -= self.notional_per_trade
                        current_position = self.get_current_position_size()
                        abs_current_position = abs(current_position)
                        self.current_entry_order_id = None
                        logger.info(
                            f'Order filled | {self.symbol} | sell | Notional: {self.notional_per_trade} | Position: {round(current_position, 2)} | Abs Position: {round(abs_current_position, 2)} | Max Notional: {self.max_notional} | Change Since Update: {round(self.position_change_since_update, 2)}')
                        print(
                            f'Order filled | {self.symbol} | Current position: {abs_current_position}/{self.max_notional}')
                        await asyncio.sleep(0.5)
                    elif 'order does not exist' in str(e).lower():
                        logger.info(f'Order {self.current_entry_order_id} no longer exists or has been filled: {e}')
                        self.current_entry_order_id = None
                    elif 'modification requests' in str(e).lower():
                        logger.info(f'Modification Requests exceeded limit for {self.symbol}, pausing for 0.5 seconds')
                        await asyncio.sleep(0.5)
                    else:
                        logger.info(f'Error while managing order: {e}')

                # Exit
                try:
                    if not(self.position_change_since_update > self.max_notional):
                        if self.current_exit_order_id is None and exit_limit_price != original_exit_limit_price:
                            self.log_order_action('buy', ma_entry_spread, ma_exit_spread, latest_row, entry_limit_price,
                                                exit_limit_price, fr_adjustment_factor, buy_spread_ma, sell_spread_ma,
                                                buy_spread_sd, sell_spead_sd)
                            self.current_exit_order_id = await self.place_limit_order(trade_symbol, 'buy',
                                                                                    self.contracts_per_trade,
                                                                                    exit_limit_price)
                            print(f"Order placed: {self.current_exit_order_id}")
                        elif self.current_exit_order_id and exit_limit_price != original_exit_limit_price:
                            self.log_order_action('buy', ma_entry_spread, ma_exit_spread, latest_row, entry_limit_price,
                                                exit_limit_price, fr_adjustment_factor, buy_spread_ma, sell_spread_ma,
                                                buy_spread_sd, sell_spead_sd)

                            self.current_exit_order_id = await self.edit_limit_order(self.current_exit_order_id,
                                                                                    trade_symbol,
                                                                                    'buy',
                                                                                    self.contracts_per_trade,
                                                                                    exit_limit_price)
                        original_exit_limit_price = exit_limit_price
                except Exception as e:
                    if any(msg in str(e) for msg in ['Your order has already been filled or canceled',
                                                     'Order cancellation failed as the order has been filled, canceled or does not exist',
                                                     'order not exists or too late to replace']):
                        
                        self.position_change_since_update += self.notional_per_trade
                        current_position = self.get_current_position_size()
                        abs_current_position = abs(current_position)
                        self.current_exit_order_id = None
                        logger.info(
                            f'Order filled | {self.symbol} | buy | Notional: {self.notional_per_trade} | Position: {round(current_position, 2)} | Abs Position: {round(abs_current_position, 2)} | Max Notional: {self.max_notional} | Change Since Update: {round(self.position_change_since_update, 2)}')
                        print(
                            f'Order filled | {self.symbol} | Current position: {abs_current_position}/{self.max_notional}')
                        await asyncio.sleep(0.5)
                    elif 'order does not exist' in str(e).lower():
                        logger.info(f'Order {self.current_exit_order_id} no longer exists or has been filled: {e}')
                        self.current_exit_order_id = None
                    elif 'modification requests' in str(e).lower():
                        logger.info(f'Modification Requests exceeded limit for {self.symbol}, pausing for 0.5 seconds')
                        await asyncio.sleep(0.5)
                    else:
                        logger.info(f'Error while managing order: {e}')
                self.previous_entry_spread = current_entry_spread
                self.previous_exit_spread = current_exit_spread

bot_configs = [
    {'exchange_perp1': 'bybit', 'exchange_perp2': 'okx', 'symbol': 'MOODENG', 'notional_per_trade': 50, 'bot_id': 'MOODENG', 'max_notional': 500, 'spreadThreshold': -0.1}
]

# This function is run only when the bot is run as a standalone script cmd python HighFrequencyBot_PP.py
async def run_bots():
    bots = [TradingBot(**config) for config in bot_configs]
    try:
        await asyncio.gather(*(bot.main() for bot in bots))
    finally:
        for bot in bots:
            bot.csv_logger.close()


def main():
    try:
        asyncio.run(run_bots())
    except KeyboardInterrupt:
        print("run_bot shutting down...")


if __name__ == "__main__":
    main()