from HighFrequencyBotV3 import TradingBot, asyncio

bot_configs = [
    # {'symbol': 'CTC', 'notional_per_trade': 100, 'bot_id': 'CTC', 'max_notional': 400},
    # {'symbol': 'FTM', 'notional_per_trade': 100, 'bot_id': 'FTM', 'max_notional': 400},
    # {'symbol': 'SUI', 'notional_per_trade': 100, 'bot_id': 'SUI', 'max_notional': 400}
    {'symbol': 'DOGS', 'notional_per_trade': 100, 'bot_id': 'DOGS', 'max_notional': 400},
    {'symbol': 'AAVE', 'notional_per_trade': 100, 'bot_id': 'AAVE', 'max_notional': 400},
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