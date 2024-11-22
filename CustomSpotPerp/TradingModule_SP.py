import asyncio
import ccxt.async_support as ccxt_async
from HighFrequencyBot_SP import TradingBot
import config


def get_user_inputs():
    # Get perpetual exchange
    perp_exchange = input("Enter the Perpetual exchange (OKX/BINANCE/GATE): ").lower()
    while perp_exchange not in ['okx', 'binance', 'gate']:
        print("Invalid exchange. Please enter OKX or BINANCE or GATE.")
        perp_exchange = input("Enter the Perpetual exchange (OKX/BINANCE/GATE): ").lower()

    # Get spot exchange
    spot_exchange = input("Enter the Spot exchange (OKX/BINANCE/GATE): ").lower()
    while spot_exchange not in ['okx', 'binance', 'gate']:
        print("Invalid exchange. Please enter OKX or BINANCE or GATE.")
        spot_exchange = input("Enter the Spot exchange (OKX/BINANCE/GATE): ").lower()

    # Get symbol (will be used as bot_id too)
    symbol = input("Enter the trading symbol (e.g., NEIRO): ").upper()

    # Get trading side
    side = input("Enter the side (buy/sell): ").lower()
    while side not in ['buy', 'sell']:
        print("Invalid side. Please enter 'buy' or 'sell'.")
        side = input("Enter the side (buy/sell): ").lower()

    # Get spread threshold as percentage
    spread_threshold = float(input("Enter the spread threshold (0.1 would be 10 bps): "))
    # Convert percentage to decimal

    return {
        'exchange_perp': perp_exchange,
        'exchange_spot': spot_exchange,
        'symbol': symbol,
        'notional_per_trade': 50,  # Pre-set value
        'bot_id': symbol,  # Same as symbol
        'max_notional': 500,  # Pre-set value
        'spreadThreshold': spread_threshold,
        'side': side
    }


async def initialize_exchange(exchange_name, is_perp=True):
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
    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")

    await exchange.load_markets()
    return exchange


async def run_bot(bot_config):
    perp_exchange = None
    spot_exchange = None
    bot = None

    try:
        # Initialize exchanges
        perp_exchange = await initialize_exchange(bot_config['exchange_perp'], is_perp=True)
        spot_exchange = await initialize_exchange(bot_config['exchange_spot'], is_perp=False)

        # Create bot instance
        bot = TradingBot(
            exchange_perp=bot_config['exchange_perp'],
            exchange_spot=bot_config['exchange_spot'],
            symbol=bot_config['symbol'],
            notional_per_trade=bot_config['notional_per_trade'],
            bot_id=bot_config['bot_id'],
            max_notional=bot_config['max_notional'],
            spreadThreshold=bot_config['spreadThreshold'],
            side=bot_config['side']
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
        if perp_exchange:
            try:
                await perp_exchange.close()
            except Exception as e:
                print(f"Error closing perpetual exchange: {str(e)}")

        if spot_exchange:
            try:
                await spot_exchange.close()
            except Exception as e:
                print(f"Error closing spot exchange: {str(e)}")


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