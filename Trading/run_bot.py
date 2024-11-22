from HighFrequencyBotV3 import TradingBot, asyncio

bot_configs = [
    {'symbol': 'AR', 'notional_per_trade': 50, 'bot_id': 'AR', 'max_notional': 1000},
    {'symbol': 'AUCTION', 'notional_per_trade': 50, 'bot_id': 'AUCTION', 'max_notional': 1000},
    # {'symbol': 'ALPHA', 'notional_per_trade': 50, 'bot_id': 'ALPHA', 'max_notional': 1000},
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