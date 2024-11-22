from HighFrequencyBotV3 import TradingBot, asyncio

bot_configs = [
    {'symbol': 'NEIRO', 'notional_per_trade': 50, 'bot_id': 'NEIRO', 'max_notional': 5000},
    {'symbol': 'MASK', 'notional_per_trade': 50, 'bot_id': 'MASK', 'max_notional': 5000},
    # {'symbol': 'MOODENG', 'notional_per_trade': 50, 'bot_id': 'MOODENG', 'max_notional': 5000}
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