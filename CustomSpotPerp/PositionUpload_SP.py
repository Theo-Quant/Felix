import time
import traceback
import logging
import ccxt
import redis
import json
from datetime import datetime
import config

logger = logging.getLogger(__name__)


class MarketDataUpdater:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = redis.Redis(host=host, port=port, db=0)
        self.total_position_threshold = 2500000
        self.reset_threshold = self.total_position_threshold * 0.99
        self.default_max_notional = 20000

        # Initialize OKX client
        self.okx = ccxt.okx({
            'apiKey': config.OKX_API_KEY,
            'secret': config.OKX_SECRET_KEY,
            'password': config.OKX_PASSPHRASE,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

        # Initialize Binance client
        self.binance = ccxt.binance({
            'apiKey': config.BINANCE_API_KEY,
            'secret': config.BINANCE_SECRET_KEY,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

        # Initialize Gate.io client
        self.gate = ccxt.gateio({
            'apiKey': config.GATE_API_KEY,
            'secret': config.GATE_SECRET_KEY,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

    def get_all_bot_ids(self):
        all_keys = self.redis_client.keys('Spot_Perp_bot_params_*')
        return [key.decode('utf-8').split('_')[-1] for key in all_keys if self.redis_client.type(key) == b'string']

    def update_all_bots_market_data(self):
        # Fetch balances from all exchanges
        binance_balances = self.fetch_spot_balances(self.binance)
        okx_balances = self.fetch_spot_balances(self.okx)
        gate_balances = self.fetch_spot_balances(self.gate)

        # Combine balances
        all_balances = {
            'binance': binance_balances,
            'okx': okx_balances,
            'gate': gate_balances
        }

        # Fetch prices and calculate USDT values
        usdt_values = self.fetch_spot_prices_and_calculate_values(all_balances)

        # Print or process the results
        print("\nUSDT values of positions:")
        for asset, value in usdt_values.items():
            print(f"{asset}: ${value:.2f}")

        # Calculate total position value
        total_position = sum(usdt_values.values())
        print(f"\nTotal position across all assets: ${total_position:.2f}")

        # Update market data for each bot
        for bot_id in self.get_all_bot_ids():
            binance_value = usdt_values.get(f"binance_{bot_id}", 0)
            okx_value = usdt_values.get(f"okx_{bot_id}", 0)
            gate_value = usdt_values.get(f"gate_{bot_id}", 0)

            total_value = binance_value + okx_value + gate_value

            self.update_market_data(
                bot_id,
                okx_value / okx_balances.get(bot_id, 1),  # Approximate OKX price
                binance_value / binance_balances.get(bot_id, 1),  # Approximate Binance price
                gate_value / gate_balances.get(bot_id, 1),  # Approximate Gate price
                okx_value,
                binance_value,
                gate_value,
                total_value
            )

        self.adjust_max_notional(total_position)

    def fetch_spot_balances(self, exchange):
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
            elif exchange.id in ['binance', 'okx']:
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

    def fetch_spot_prices_and_calculate_values(self, balances):
        usdt_values = {}
        for exchange_id, exchange_balances in balances.items():
            exchange = getattr(self, exchange_id)

            # Format symbols according to exchange requirements
            if exchange_id == 'gate':
                symbols = [f"{asset}/USDT" for asset in exchange_balances.keys() if asset != 'USDT']
            else:
                symbols = [f"{asset}/USDT" for asset in exchange_balances.keys() if asset != 'USDT']

            try:
                tickers = exchange.fetch_tickers(symbols)
                for asset, amount in exchange_balances.items():
                    if asset == 'USDT':
                        usdt_values[f"{exchange_id}_{asset}"] = amount
                    else:
                        symbol = f"{asset}/USDT"
                        if symbol in tickers and tickers[symbol]['last']:
                            price = tickers[symbol]['last']
                            value = amount * price
                            usdt_values[f"{exchange_id}_{asset}"] = value
                        else:
                            logger.warning(f"Could not fetch price for {symbol} on {exchange_id}")
            except Exception as e:
                logger.error(f"Error fetching prices from {exchange_id}: {str(e)}")
                traceback.print_exc()

        return usdt_values

    def update_market_data(self, bot_id, okx_mark_price, binance_mark_price, gate_mark_price,
                           okx_value, binance_value, gate_value, total_value):
        params_key = f'Spot_Perp_bot_params_{bot_id}'
        current_params = self.redis_client.get(params_key)

        if current_params:
            current_params = json.loads(current_params)
            current_params.update({
                'okx_mark_price': okx_mark_price,
                'binance_mark_price': binance_mark_price,
                'gate_mark_price': gate_mark_price,
                'okx_position_size': okx_value,
                'binance_position_size': binance_value,
                'gate_position_size': gate_value,
                'total_position_size': total_value
            })
            self.redis_client.set(params_key, json.dumps(current_params))
            print(f"Updated data for bot {bot_id}:")
            print(f"  OKX Position: ${okx_value:.2f}, Mark Price ${okx_mark_price}")
            print(f"  Binance Position: ${binance_value:.2f}, Mark Price ${binance_mark_price}")
            print(f"  Gate Position: ${gate_value:.2f}, Mark Price ${gate_mark_price}")
            print(f"  Total Position: ${total_value:.2f}")

    def adjust_max_notional(self, total_position):
        for bot_id in self.get_all_bot_ids():
            params_key = f'Spot_Perp_bot_params_{bot_id}'
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
    print("Monitoring balances and positions across OKX, Binance, and Gate.io")

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