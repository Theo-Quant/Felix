import sys
import asyncio
import ccxt.async_support as ccxt_async
from HighFrequencyBot_PP import TradingBot
# import config
import TradingModule_PPConfig as config

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def get_user_inputs():

    SUPPORTED_EXCHANGES = config.SUPPORT_EXCHANGES
    exchange_str = "/".join(SUPPORTED_EXCHANGES)

    # Get perpetual exchange 1
    perp1_exchange = input(f"Enter the Perpetual exchange 1 ({exchange_str}): ").lower()
    while perp1_exchange.upper() not in SUPPORTED_EXCHANGES:
        print(f"Invalid exchange. Please enter ({exchange_str}).")
        perp1_exchange = input(f"Enter the Perpetual exchange 1 ({exchange_str}):").lower()

    # Get perpetual exchange 2
    perp2_exchange = input(f"Enter the Perpetual exchange 2 ({exchange_str}): ").lower()
    while perp2_exchange.upper() not in SUPPORTED_EXCHANGES:
        print(f"Invalid exchange. Please enter ({exchange_str}).")
        perp2_exchange = input(f"Enter the Perpetual exchange 2 ({exchange_str}):").lower()

    # Get symbol (will be used as bot_id too)
    symbol = input("Enter the trading symbol (e.g., NEIRO): ").upper()

    # Get spread threshold as percentage
    # spread_threshold = float(input("Enter the spread threshold (0.1 would be 10 bps): "))
    entry_spread_threshold = float(input("Enter the entry bound (0.1 would be 10 bps): "))
    exit_spread_threshold = float(input("Enter the exit bound (0.1 would be 10 bps): "))
    # Convert percentage to decimal

    return {
        'exchange_perp1': perp1_exchange,
        'exchange_perp2': perp2_exchange,
        'symbol': symbol,
        'notional_per_trade': 50,  # Pre-set value
        'bot_id': symbol,  # Same as symbol
        'max_notional': 500,  # Pre-set value
        'entry_spread_threshold': entry_spread_threshold,
        'exit_spread_threshold': exit_spread_threshold,
    }

async def initialize_exchange(exchange_name, is_perp=True):
    is_perp = True # This is a perp trading bot
    if exchange_name == 'okx':
        exchange = ccxt_async.okx({
            'apiKey': config.OKX_API_KEY,
            'secret': config.OKX_SECRET_KEY,
            'password': config.OKX_PASSPHRASE,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap' if is_perp else 'spot'}
        })
    elif exchange_name == 'binance':
        exchange = ccxt_async.binance({
            'apiKey': config.BINANCE_API_KEY,
            'secret': config.BINANCE_SECRET_KEY,
            'enableRateLimit': True,
            'options': {'defaultType': 'future' if is_perp else 'spot'}
        })
    elif exchange_name == 'gate':
        exchange = ccxt_async.gate({
            'apiKey': config.GATE_API_KEY,
            'secret': config.GATE_SECRET_KEY,
            'enableRateLimit': True,
            'options': {'defaultType': 'future' if is_perp else 'spot'}
        })
    elif exchange_name == 'bitget':
        exchange = ccxt_async.bitget({
            'apiKey': config.BITGET_API_KEY,
            'secret': config.BITGET_SECRET_KEY,
            'password': config.BITGET_PASSPHRASE,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap' if is_perp else 'spot'} # bitget: 'swap': 'usdt_futures', 'future': 'coin_futures', 
        })
    elif exchange_name == 'bybit':
        exchange = ccxt_async.bybit({
            'apiKey': config.BYBIT_API_KEY,
            'secret': config.BYBIT_SECRET_KEY,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap' if is_perp else 'spot'}
        })
    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")

    await exchange.load_markets()
    return exchange


async def run_bot(bot_config):
    perp1_exchange = None
    perp2_exchange = None
    bot = None

    try:
        # Initialize exchanges
        perp1_exchange = await initialize_exchange(bot_config['exchange_perp1'], is_perp=True)
        perp2_exchange = await initialize_exchange(bot_config['exchange_perp2'], is_perp=False)

        # Create bot instance
        bot = TradingBot(
            exchange_perp1=bot_config['exchange_perp1'],
            exchange_perp2=bot_config['exchange_perp2'],
            symbol=bot_config['symbol'],
            notional_per_trade=bot_config['notional_per_trade'],
            bot_id=bot_config['bot_id'],
            max_notional=bot_config['max_notional'],
            entry_spread_threshold=bot_config['entry_spread_threshold'],
            exit_spread_threshold=bot_config['exit_spread_threshold'],
        )

        # Run the bot
        await bot.main()

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

    finally:
        # Clean up resources
        if bot and hasattr(bot, 'csv_logger'):
            try:
                bot.csv_logger.close()
            except Exception as e:
                print(f"Error closing CSV logger: {str(e)}")

        # Close exchanges
        if perp1_exchange:
            try:
                await perp1_exchange.close()
            except Exception as e:
                print(f"Error closing perpetual exchange 1: {str(e)}")

        if perp2_exchange:
            try:
                await perp2_exchange.close()
            except Exception as e:
                print(f"Error closing perpetual exchange 2: {str(e)}")


def main():
    print("Welcome to the High Frequency Trading Bot")
    print("Please ensure correct parameters are set up already for the notional per trade and max notional.")

    bot_config = get_user_inputs()

    print("\nConfig to be used:")
    for key, value in bot_config.items():
        print(f"{key}: {value}")

    try:
        asyncio.run(run_bot(bot_config))
    except KeyboardInterrupt:
        print("\nBot shutting down due to keyboard interrupt...")
    except Exception as e:
        print(f"\nBot shutting down due to error: {str(e)}")
    finally:
        print("Shutdown complete.")


if __name__ == "__main__":
    main()