from HighFrequencyBotV3 import TradingBot, asyncio

bot_configs = [
    {'symbol': 'STORJ', 'notional_per_trade': 50, 'bot_id': 'STORJ', 'max_notional': 5000},
    {'symbol': 'TURBO', 'notional_per_trade': 50, 'bot_id': 'TURBO', 'max_notional': 5000},
    {'symbol': 'MEW', 'notional_per_trade': 50, 'bot_id': 'MEW', 'max_notional': 5000}
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