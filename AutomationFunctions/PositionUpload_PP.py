import os
import time
import traceback
import logging
import ccxt
import redis
import json
from datetime import datetime
from dotenv import load_dotenv
# import config
# import TradingModule_PPConfig as config

logger = logging.getLogger(__name__)

load_dotenv()
OKX_API_KEY = os.getenv('OKX_API_KEY')
OKX_SECRET_KEY = os.getenv('OKX_SECRET_KEY')
OKX_PASSPHRASE = os.getenv('OKX_PASSPHRASE')

BYBIT_API_KEY = os.getenv('BYBIT_API_KEY')
BYBIT_SECRET_KEY = os.getenv('BYBIT_SECRET_KEY')

class MarketDataUpdater:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = redis.Redis(host=host, port=port, db=0)
        self.total_position_threshold = 2500000
        self.reset_threshold = self.total_position_threshold * 0.99
        self.default_max_notional = 20000

        # Initialize OKX client
        self.okx = ccxt.okx({
            'apiKey': OKX_API_KEY,
            'secret': OKX_SECRET_KEY,
            'password': OKX_PASSPHRASE,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })

        # Initialize Bybit client
        self.bybit = ccxt.bybit({
            'apiKey': BYBIT_API_KEY,
            'secret': BYBIT_SECRET_KEY,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })

    def get_all_bot_ids(self):
        all_keys = self.redis_client.keys('Perp_Perp_bot_params_*')
        return [key.decode('utf-8').split('_')[-1] for key in all_keys if self.redis_client.type(key) == b'string']

    def update_all_bots_market_data(self):
        # Fetch balances from all exchanges
        okx_balances = self.fetch_perp_balances(self.okx)
        bybit_balances = self.fetch_perp_balances(self.bybit)

        # Combine balances
        all_balances = {
            'okx': okx_balances,
            'bybit': bybit_balances
        }

        # Print balances
        print("\nBalances across OKX, Bybit:")
        for exchange, balances in all_balances.items():
            print(f"{exchange}: {balances}")
        # Print balances

        # Fetch prices and calculate USDT values
        usdt_values = self.fetch_perp_prices_and_calculate_values(all_balances)

        # Print or process the results
        print("\nUSDT values of positions:")
        for asset, value in usdt_values.items():
            print(f"{asset}: ${value[0]:.2f}")

        # Calculate total position value
        total_position = sum(value[0] for value in usdt_values.values())
        print(f"\nTotal position across all assets: ${total_position:.2f}")

        # Update market data for each bot
        for bot_id in self.get_all_bot_ids():
            okx_value = usdt_values.get(f"okx_{bot_id}/USDT:USDT", [0,0])[0]
            bybit_value = usdt_values.get(f"bybit_{bot_id}/USDT:USDT", [0,0])[0]

            total_value = okx_value + bybit_value

            # Change this
            okx_mark_price = usdt_values.get(f"okx_{bot_id}/USDT:USDT", [0,0])[1]
            bybit_mark_price = usdt_values.get(f"bybit_{bot_id}/USDT:USDT", [0,0])[1]
            if okx_mark_price == 0 or bybit_mark_price == 0:
                okx_mark_price = self.okx.fetch_ticker(f'{bot_id}/USDT:USDT')['last']
            self.update_market_data(
                bot_id=bot_id,
                okx_mark_price=okx_mark_price,
                okx_value=okx_value,
                bybit_mark_price=bybit_mark_price,
                bybit_value=bybit_value,
                total_value=total_value
            )

        self.adjust_max_notional(total_position)

    def fetch_perp_balances(self, exchange):
        try:
            balances = exchange.fetch_balance()
            if exchange.id == 'gateio':
                return {
                    asset: float(balance['free']) + float(balance['used'])
                    for asset, balance in balances.items()
                    if isinstance(balance, dict)
                       and 'free' in balance
                       and 'used' in balance
                       and (float(balance['free']) + float(balance['used'])) > 0
                       and asset not in ['info', 'timestamp', 'datetime', 'free', 'used', 'total']
                }
            elif exchange.id in ['binance', 'okx', 'bybit']:
                return {
                    asset: float(balance['free']) + float(balance['used'])
                    for asset, balance in balances.items()
                    if asset not in ['info', 'timestamp', 'datetime', 'free', 'used', 'total']
                       and isinstance(balance, dict)
                       and 'free' in balance
                       and 'used' in balance
                       and (float(balance['free']) + float(balance['used'])) > 0
                }
            else:
                logger.error(f"Unsupported exchange: {exchange.id}")
                return {}
        except Exception as e:
            logger.error(f"Error fetching balances from {exchange.id}: {str(e)}")
            traceback.print_exc()
            return {}

    def fetch_perp_prices_and_calculate_values(self, balances):
        usdt_values = {}
        for exchange_id, _ in balances.items():
            exchange = getattr(self, exchange_id)

            try:
                positions = exchange.fetch_positions()
                for position in positions:
                    asset = position['symbol']
                    value = position['notional']
                    mark_price = position['markPrice']
                    usdt_values[f"{exchange_id}_{asset}"] = value, mark_price
            except Exception as e:
                logger.error(f"Error fetching positions from {exchange_id}: {str(e)}")
                traceback.print_exc()
        
        return usdt_values

    def update_market_data(self, bot_id, okx_mark_price, okx_value, bybit_mark_price,
                           bybit_value, total_value):
        params_key = f'Perp_Perp_bot_params_{bot_id}'
        current_params = self.redis_client.get(params_key)

        if current_params:
            current_params = json.loads(current_params)
            current_params.update({
                'okx_mark_price': okx_mark_price,
                'okx_position_size': okx_value,
                'bybit_mark_price': bybit_mark_price,
                'bybit_position_size': bybit_value,
                'total_position_size': total_value,
                'mark_price': okx_mark_price,
                'position_size': okx_value
            })
            self.redis_client.set(params_key, json.dumps(current_params))
            print(f"Updated data for bot {bot_id}:")
            print(f"  OKX Position: ${okx_value:.2f}, Mark Price ${okx_mark_price}")
            print(f"  Bybit Position: ${bybit_value:.2f}, Mark Price ${bybit_mark_price}")
            print(f"  Total Position: ${total_value:.2f}")

    def adjust_max_notional(self, total_position):
        for bot_id in self.get_all_bot_ids():
            params_key = f'Perp_Perp_bot_params_{bot_id}'
            current_params = self.redis_client.get(params_key)
            if current_params:
                current_params = json.loads(current_params)
                current_position = current_params.get('total_position_size', 0)
                default_max_notional = current_params.get('default_max_notional', self.default_max_notional)

                if total_position >= self.total_position_threshold:
                    new_max_notional = current_position * 0.95
                elif total_position <= self.reset_threshold:
                    new_max_notional = default_max_notional
                else:
                    continue  # No change needed

                current_params['max_notional'] = new_max_notional
                self.redis_client.set(params_key, json.dumps(current_params))
                print(f"Adjusted max notional for bot {bot_id}: ${new_max_notional:.2f}")


def run_periodic_update():
    try:
        updater = MarketDataUpdater()
        updater.update_all_bots_market_data()
    except Exception as e:
        logger.error(f"Error in run_periodic_update: {str(e)}")
        print("Traceback:")
        traceback.print_exc()
        logger.error("Restarting...")


if __name__ == "__main__":
    print("Market Data Updater Started")
    print("Monitoring balances and positions across OKX, Bybit")

    while True:
        try:
            run_periodic_update()
            time.sleep(60)  # Run every 60 seconds
        except KeyboardInterrupt:
            print("\nScript terminated by user")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            print("Traceback:")
            traceback.print_exc()
            logger.error("Restarting...")
            time.sleep(5)  # Wait for 5 seconds before restarting