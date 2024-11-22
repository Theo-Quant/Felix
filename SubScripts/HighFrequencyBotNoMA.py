import asyncio
import json
import ccxt.async_support as ccxt_async
import redis
import time
import logging
import random
import string
import config
import csv
import os
from datetime import datetime


#region Initialization
logger = config.setup_logger('HighFrequencyBotV2')
redis_client = redis.Redis(host='localhost', port=6379, db=0)

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
    'apiKey': config.OKX_API_KEY,
    'secret': config.OKX_SECRET_KEY,
    'password': config.OKX_PASSPHRASE,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap'
    }
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
        onedrive_path = r"C:\Users\Lewis Lu\OneDrive\Documents\tradelogs"
        filename = os.path.join(onedrive_path, f"bot_{self.bot_id}_{timestamp}_V2.csv")
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        file_exists = os.path.isfile(filename)
        self.current_file = open(filename, 'a', newline='')  # 'a' for append mode
        self.current_writer = csv.writer(self.current_file)

        # Get the current row count
        self.current_file.seek(0, os.SEEK_END)
        self.row_count = self.current_file.tell() // 100

    def log(self, side, ma_entry_spread, entry_spread, ma_exit_spread, exit_spread, limit_order, fr_factor, entry_bound, exit_bound, impact_bid_price_okx, impact_ask_price_binance, buy_spread_ma, sell_spread_ma, buy_spread_sd, sell_spead_sd):
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
            impact_bid_price_okx,
            impact_ask_price_binance,
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
    def __init__(self, symbol, notional_per_trade, bot_id, max_notional):
        self.csv_logger = CSVLogger(bot_id)
        self.symbol = symbol
        self.params_key = f'bot_params_{bot_id}'
        self.default_params = {
            'okx_contracts_per_trade': 1,
            'ma': 10,
            'max_notional': max_notional,
            'max_positions': 1,
            'notional_per_trade': notional_per_trade,
            'std_coeff': 1,
            'min_width': 0.07
        }
        self.params = self.default_params.copy()
        self.max_positions = None
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
        bot_id_filter = BotIdFilter(self.bot_id)
        logger.addFilter(bot_id_filter)
        logger.info(f'Bot ID: {bot_id} | Max Notional: {self.max_notional} | Notional per Trade: {self.notional_per_trade} / {self.okx_contracts_per_trade} contracts | Percent to max notional: {round(100*self.position_size / self.max_notional, 3)}%')

    def update_params(self):
        params_json = redis_client.get(self.params_key)
        if params_json:
            updated_params = json.loads(params_json)
            self.params.update(updated_params)

        # Update instance variables
        self.contract_size = config.OKX_CONTRACT_SZ.get(f'{self.symbol}-USDT-SWAP')
        self.notional_per_trade = self.params['notional_per_trade']
        self.max_notional = self.params['max_notional']
        self.ma = self.params['ma']
        self.std_coeff = self.params['std_coeff']
        self.min_width = self.params['min_width']
        self.mark_price = self.params['mark_price']
        self.position_size = self.params['position_size']

        # Only update okx_contracts_per_trade if mark_price and contract_size are set
        if self.mark_price is not None:
            self.okx_contracts_per_trade = round(self.notional_per_trade / (self.mark_price * self.contract_size))
        else:
            self.okx_contracts_per_trade = 0
            print("Warning: mark_price or contract_size not set. Cannot update okx_contracts_per_trade.")

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
        return 'SpreadArbTest9NoMA' + ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    @staticmethod
    def get_current_time_ms():
        return int(time.time() * 1000)

    async def initialize_clients(self):
        print('initializing markets')
        await binance.load_markets()
        await okx.load_markets()

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
        values = [item[key] for item in data]
        return sum(values) / len(values) if values else None

    @staticmethod
    def calculate_sd(data, key, ma):
        values = [item[key] for item in data]
        variance = sum((x - ma) ** 2 for x in values) / len(values) if values else None
        return variance ** 0.5 if variance else None

    @staticmethod
    def should_continue_trading():
        stop_flag = redis_client.get('stop_bot')
        return stop_flag != b'true'

    def get_trend_data(self, coin, fr_adjustment_factor):
        data = redis_client.hget('trend_data', f'{coin}/USDT')
        if data is None:
            # If the data is none (if the data is pulled during the flush, then return a spread of 1% on both ends)
            return 1, -1, None
        coin_data = json.loads(data)

        sell_bound = max((coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M'])/2 + coin_data['sell_spread_sd_M']*self.std_coeff, self.min_width/2 + (coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M'])/2) + max(fr_adjustment_factor, 0)
        # print(
        #    f"buy_ma: {coin_data['buy_spread_ma_M']:.4f}, sell_ma: {coin_data['sell_spread_ma_M']:.4f}, sell_sd: {coin_data['sell_spread_sd_M']:.4f}, std_coeff: {self.std_coeff:.2f}, min_width: {self.min_width:.4f}, fr_adj: {fr_adjustment_factor:.4f}, sell_bound: {sell_bound:.4f}")
        buy_bound = min((coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M'])/2 - coin_data['buy_spread_sd_M']*self.std_coeff, (coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M'])/2 - self.min_width/2) + min(fr_adjustment_factor, 0)

        return sell_bound, buy_bound, coin_data, coin_data['buy_spread_ma_M'], coin_data['sell_spread_ma_M'], coin_data['buy_spread_sd_M'], coin_data['sell_spread_sd_M']

    async def place_okx_limit_order(self, symbol, side, quantity, price):
        # Add in the client ID for the specific strategy
        try:
            client_order_id = str(self.generate_client_order_id())
            order = await okx.create_order(symbol, "post_only", side, quantity, price,
                                           params={'clientOrderId': client_order_id})
            return order['id']
        except Exception as e:
            print(f"Failed to place OKX limit order: {e}")
            return None

    async def edit_okx_limit_order(self, order_id, symbol, side, quantity, price):
        order = await okx.edit_order(order_id, symbol, 'post_only', side, quantity, price)
        return order['id']

    async def cancel_okx_order(self, order_id, symbol):
        result = await okx.cancel_order(order_id, symbol)
        print(f"Cancelled order {order_id} for {symbol}")
        return result

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
            latest_row["impact_bid_price_okx"],
            latest_row["impact_ask_price_binance"],
            buy_spread_ma,
            sell_spread_ma,
            buy_spread_sd,
            sell_spead_sd
        )
        if side == 'sell':
            print(
                f'({datetime.now()}) | Bot {self.bot_id} | {side} order | sell ma: {round(ma_entry_spread, 3)} | sell spread: {round(latest_row["entry_spread"], 3)} | limit order: {round(entry_limit_price, 6)} | fr factor: {fr_adjustment_factor} | bollinger bands: ({round(self.entry_bound, 5)}, {round(self.exit_bound, 5)}) | price (O/B): ({latest_row["impact_bid_price_okx"]}/{latest_row["impact_ask_price_binance"]})')
        elif side == 'buy':
            print(
                f'({datetime.now()}) | Bot {self.bot_id} | {side} order | buy ma: {round(ma_exit_spread, 3)} | buy spread: {round(latest_row["exit_spread"], 3)} | limit order: {round(exit_limit_price, 6)} | fr factor: {fr_adjustment_factor} | bollinger bands: ({round(self.entry_bound, 5)}, {round(self.exit_bound, 5)}) | price (O/B): ({latest_row["impact_ask_price_okx"]}/{latest_row["impact_bid_price_binance"]})')

    def choose_direction(self, current_entry_spread, current_exit_spread):
        """
        Determine the direction for the trade based on position limits and spread distances.
        """
        # Check if we've reached the maximum notional value
        if abs(self.get_current_position_size()) + self.notional_per_trade > self.max_notional:
            return 'sell' if self.position_size > 0 else 'buy'
        # If neither limit is reached, decide based on spread distances
        entry_distance = self.entry_bound - current_entry_spread
        exit_distance = current_exit_spread - self.exit_bound
        if self.max_notional == 0 and self.notional_per_trade == 0:
            return None
        return 'sell' if entry_distance < exit_distance else 'buy'

    async def stop_trading(self):
        logger.info(f"Bot {self.bot_id} stopping trading operations.")
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

        def within_25_percent(previous, current):
            if not previous or not current:
                return False
            return abs(current - previous) / previous <= 0.25

        entry_smoothness_check = within_25_percent(self.previous_entry_spread, current_entry_spread)
        exit_smoothness_check = within_25_percent(self.previous_exit_spread, current_exit_spread)

        if self.is_paused():
            logger.info(f"Bot {self.bot_id} temporarily adjusting limits due to recent server overload.")
            entry_limit_price += self.adjustment_value
            exit_limit_price -= self.adjustment_value
        else:
            if latest_row['entry_spread'] < self.entry_bound or not entry_smoothness_check or ma_entry_spread < self.entry_bound:
                entry_limit_price += self.adjustment_value
            if latest_row['exit_spread'] > self.exit_bound or not exit_smoothness_check or ma_exit_spread > self.exit_bound:
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
        # initially pull the latest row once to get a 10% adjustment buffer value
        self.adjustment_value = (self.get_latest_row(f'combined_data_{self.symbol}'))['best_bid_price_okx'] * 0.1

        while True:
            if not self.should_continue_trading():
                logger.info(f"Bot {self.bot_id} stopping due to stop_run_bot flag.")
                await self.stop_trading()
                break
            self.update_params()
            latest_data = self.get_latest_data(f'combined_data_{self.symbol}', count=self.ma)
            latest_row = self.get_latest_row(f'combined_data_{self.symbol}')
            # Check if current time is within the first 5 minutes of a 4-hour interval
            current_time = datetime.now()
            if current_time.hour % 4 == 0 and current_time.minute < 5:
                fr_adjustment_factor = 0
            else:
                # Fetch the funding rate adjustment factor from Redis for the specific coin
                fr_adjustment_factor = 0
                json_data = redis_client.get(f'funding_rates:{self.symbol}/USDT')
                if json_data:
                    data = json.loads(json_data)
                    fr_adjustment_factor = data.get('fr_adjustment_factor', 0)

            self.entry_bound, self.exit_bound, coin_data, buy_spread_ma, sell_spread_ma, buy_spread_sd, sell_spead_sd = self.get_trend_data(self.symbol, fr_adjustment_factor)

            if latest_data and len(latest_data) >= self.ma:
                spreads_and_limits = self.calculate_spreads_and_limits(latest_data, latest_row)
                ma_entry_spread = spreads_and_limits["ma_entry_spread"]
                ma_exit_spread = spreads_and_limits["ma_exit_spread"]
                current_entry_spread = spreads_and_limits["current_entry_spread"]
                current_exit_spread = spreads_and_limits["current_exit_spread"]
                entry_limit_price = spreads_and_limits["entry_limit_price"]
                exit_limit_price = spreads_and_limits["exit_limit_price"]
                side = self.choose_direction(current_entry_spread, current_exit_spread)

                if side == 'buy':
                    limit_price = exit_limit_price
                else:
                    limit_price = entry_limit_price

                if side is None:
                    logger.info(f"Bot {self.bot_id} has no trade opportunity at this time.")
                    await asyncio.sleep(0.02)
                    continue

                try:
                    if self.current_okx_order_id is None:
                        self.log_order_action(side, ma_entry_spread, ma_exit_spread, latest_row, entry_limit_price,
                                              exit_limit_price, fr_adjustment_factor, buy_spread_ma, sell_spread_ma, buy_spread_sd, sell_spead_sd)
                        self.current_okx_order_id = await self.place_okx_limit_order(f'{self.symbol}-USDT-SWAP', side,
                                                                                     self.okx_contracts_per_trade,
                                                                                     limit_price)
                        print(f"Order placed: {self.current_okx_order_id}")
                    else:
                        self.log_order_action(side, ma_entry_spread, ma_exit_spread, latest_row, entry_limit_price,
                                              exit_limit_price, fr_adjustment_factor, buy_spread_ma, sell_spread_ma, buy_spread_sd, sell_spead_sd)

                        # Code below for modifying/cancelling the existing order and aligning them with the current data
                        if self.previous_side != side:
                            await self.cancel_okx_order(self.current_okx_order_id, f'{self.symbol}-USDT-SWAP')
                            self.current_okx_order_id = await self.place_okx_limit_order(f'{self.symbol}-USDT-SWAP',
                                                                                         side,
                                                                                         self.okx_contracts_per_trade,
                                                                                         limit_price)
                        # If the sides didn't change:
                        else:
                            if side == 'sell':
                                spread_difference = (ma_entry_spread - self.entry_bound) / self.entry_bound
                                modify = spread_difference > -0.35  # -35%
                            else:  # side == 'buy'
                                spread_difference = (self.exit_bound - ma_exit_spread) / ma_exit_spread
                                modify = spread_difference > -0.35  # -35%

                            if modify:
                                self.current_okx_order_id = await self.edit_okx_limit_order(self.current_okx_order_id,
                                                                                            f'{self.symbol}-USDT-SWAP',
                                                                                            side,
                                                                                            self.okx_contracts_per_trade,
                                                                                            limit_price)
                            else:
                                print(f'{self.symbol} - Spread not in modification zone, skipping order update.')
                            # else:
                            #     logger.info(f'{self.symbol}-USDT - Current {side} spread is '
                            #                 f'{current_entry_spread if side == "sell" else current_exit_spread} '
                            #                 f'and bound is {self.entry_bound if side == "sell" else self.exit_bound}. '
                            #                 f'Difference is {spread_difference:.2%}. Skipping order modification')

                except Exception as e:
                    if 'Your order has already been filled or canceled' in str(e):
                        self.current_okx_order_id = None
                        if self.previous_side == 'buy':
                            self.position_change_since_update += self.notional_per_trade
                        elif self.previous_side == 'sell':
                            self.position_change_since_update -= self.notional_per_trade
                        current_position = self.get_current_position_size()
                        abs_current_position = abs(current_position)
                        logger.info(
                            f'Order filled | {self.symbol} | {self.previous_side} | Notional: {self.notional_per_trade} | Position: {round(current_position, 2)} | Abs Position: {round(abs_current_position, 2)} | Max Notional: {self.max_notional} | Change Since Update: {round(self.position_change_since_update, 2)}')
                        print(
                            f'Order filled | {self.symbol} | Current position: {abs_current_position}/{self.max_notional}')
                        await asyncio.sleep(0.5)
                    elif 'Order cancellation failed as the order has been filled, canceled or does not exist' in str(e):
                        logger.info(f'Order {self.current_okx_order_id} no longer exists or has been filled: {e}')
                        self.current_okx_order_id = None
                    elif 'Number of modification requests that are currently in progress for an order cannot exceed 3 times' in str(e):
                        logger.info(f'Modification Requests for exceeded limit for {self.symbol}, pausing for 0.5 seconds')
                        await asyncio.sleep(0.5)
                    else:
                        logger.info(f'Error while managing order: {e}')
                self.previous_entry_spread = current_entry_spread
                self.previous_exit_spread = current_exit_spread
                self.previous_side = side
            await asyncio.sleep(0.01)


"""
Symbol: Symbol being traded
quantity: Quantity in contract size for OKX
ma: Window size for the rolling average (based on number of rows in redis)
max_positions: Total Number of positions to have per coin
buy: Current open buy positions, set to 0 if starting a new bot.
bot_id: Name of coin used for documenting purposes
"""


# # TODO: Add in a max notional check to ensure we don't let partial fills get too many positions
bot_configs = [
    {'symbol': 'ORDI', 'notional_per_trade': 75, 'bot_id': 'ORDI', 'max_notional': 15000}
]


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