"""
Routinely checks and monitors total position size across positions in OKX and Hyperliquid.
"""


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
            'apiKey': config.OKX_API_KEY_HYPERLIQUIDSUB,
            'secret': config.OKX_SECRET_KEY_HYPERLIQUIDSUB,
            'password': config.OKX_PASSPHRASE_HYPERLIQUIDSUB,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

        # Initialize Hyperliquid client
        self.hyperliquid = ccxt.hyperliquid({
            'apiKey': config.HYPERLIQUID_API_KEY,
            'secret': config.HYPERLIQUID_SECRET_KEY,
            'enableRateLimit': True,
            'walletAddress': config.HYPERLIQUID_SUB_HYPEArb
        })

    def get_all_bot_ids(self):
        all_keys = self.redis_client.keys('HyperliquidOKX_bot_params_*')
        return [key.decode('utf-8').split('_')[-1] for key in all_keys if self.redis_client.type(key) == b'string']

    def update_all_bots_market_data(self):
        # Fetch balances from exchanges
        hyperliquid_balances = self.hyperliquid.fetchBalance({'type': 'spot'})
        okx_balances = self.fetch_spot_balances(self.okx)

        # Combine balances
        all_balances = {
            'hyperliquid': hyperliquid_balances,
            'okx': okx_balances
        }

        # Fetch prices and calculate USDT values
        usdt_values = self.fetch_spot_prices_and_calculate_values(all_balances)

        # Print results
        print("\nUSDT values of positions:")
        for asset, value in usdt_values.items():
            print(f"{asset}: ${value:.2f}")

        # Calculate total position value
        total_position = sum(usdt_values.values())
        print(f"\nTotal position across all assets: ${total_position:.2f}")

        # Update market data for each bot
        for bot_id in self.get_all_bot_ids():
            hyperliquid_value = usdt_values.get(f"hyperliquid_{bot_id}", 0)
            okx_value = usdt_values.get(f"okx_{bot_id}", 0)

            total_value = hyperliquid_value + okx_value

            self.update_market_data(
                bot_id,
                okx_value / okx_balances.get(bot_id, 1),  # Approximate OKX price
                hyperliquid_value / hyperliquid_balances.get('total', {}).get(bot_id, 1),  # Hyperliquid price
                okx_value,
                hyperliquid_value,
                total_value
            )

        self.adjust_max_notional(total_position)

    def fetch_spot_balances(self, exchange):
        try:
            if exchange.id == 'hyperliquid':
                # Return the raw balance response for Hyperliquid
                return exchange.fetchBalance({'type': 'spot'})
            else:
                # Original handling for OKX
                balances = exchange.fetch_balance()
                return {
                    asset: float(balance['free']) + float(balance['used'])
                    for asset, balance in balances.items()
                    if asset not in ['info', 'timestamp', 'datetime', 'free', 'used', 'total']
                       and isinstance(balance, dict)
                       and 'free' in balance
                       and 'used' in balance
                       and (float(balance['free']) + float(balance['used'])) > 0
                }
        except Exception as e:
            logger.error(f"Error fetching balances from {exchange.id}: {str(e)}")
            traceback.print_exc()
            return {}

    def fetch_spot_prices_and_calculate_values(self, balances):
        usdt_values = {}

        # Handle Hyperliquid balances directly
        if 'hyperliquid' in balances:
            hyperliquid_data = balances['hyperliquid']
            hl_balances = hyperliquid_data['info']['balances']
            for balance in hl_balances:
                coin = balance['coin']
                total = float(balance['total'])
                if coin == 'USDC':
                    usdt_values[f"hyperliquid_{coin}"] = total
                else:
                    entry_ntl = float(balance['entryNtl'])
                    usdt_values[f"hyperliquid_{coin}"] = entry_ntl

        # Handle OKX balances
        if 'okx' in balances:
            exchange = self.okx
            exchange_balances = balances['okx']

            if exchange_balances:
                # Filter out None and empty values
                symbols = [f"{asset}-USDT" for asset in exchange_balances.keys()
                           if
                           asset and asset != 'USDT' and asset not in ['info', 'timestamp', 'datetime', 'free', 'used',
                                                                       'total']]

                if symbols:  # Only proceed if we have valid symbols
                    try:
                        tickers = exchange.fetch_tickers(symbols)
                        for asset, amount in exchange_balances.items():
                            if not asset or asset in ['info', 'timestamp', 'datetime', 'free', 'used', 'total']:
                                continue

                            if asset == 'USDT':
                                usdt_values[f"okx_{asset}"] = amount
                            else:
                                symbol = f"{asset}-USDT"
                                if symbol in tickers and tickers[symbol]['last']:
                                    price = tickers[symbol]['last']
                                    value = amount * price
                                    usdt_values[f"okx_{asset}"] = value
                                else:
                                    logger.warning(f"Could not fetch price for {symbol} on okx")
                    except Exception as e:
                        logger.error(f"Error fetching prices from okx: {str(e)}")
                        traceback.print_exc()

        print("Final USDT values:", usdt_values)
        return usdt_values

    def update_market_data(self, bot_id, okx_mark_price, hyperliquid_mark_price,
                           okx_value, hyperliquid_value, total_value):
        params_key = f'HyperliquidOKX_bot_params_{bot_id}'
        current_params = self.redis_client.get(params_key)

        if current_params:
            current_params = json.loads(current_params)
            current_params.update({
                'okx_mark_price': okx_mark_price,
                'hyperliquid_mark_price': hyperliquid_mark_price,
                'okx_position_size': okx_value,
                'hyperliquid_position_size': hyperliquid_value,
                'total_position_size': total_value
            })
            self.redis_client.set(params_key, json.dumps(current_params))
            print(f"Updated data for bot {bot_id}:")
            print(f"  OKX Position: ${okx_value:.2f}, Mark Price ${okx_mark_price}")
            print(f"  Hyperliquid Position: ${hyperliquid_value:.2f}, Mark Price ${hyperliquid_mark_price}")
            print(f"  Total Position: ${total_value:.2f}")

    def adjust_max_notional(self, total_position):
        for bot_id in self.get_all_bot_ids():
            params_key = f'HyperliquidOKX_bot_params_{bot_id}'
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
    print("Monitoring balances and positions across Hyperliquid and OKX")

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