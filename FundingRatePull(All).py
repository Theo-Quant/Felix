import asyncio
import ccxt.async_support as ccxt
import config
from datetime import datetime, timezone
import telegram
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Initialize exchanges without API keys
okx = ccxt.okx({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

binance = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

gate = ccxt.gateio({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

# Initialize Telegram bot
bot = telegram.Bot(token=config.bot_tokenFunding)
CHAT_ID = config.chat_idFunding


async def fetch_funding_rate(exchange, symbol):
    """
    Fetches the funding rate for a given symbol from the specified exchange.

    Args:
    exchange (ccxt.Exchange): The exchange object to fetch data from.
    symbol (str): The trading symbol to fetch the funding rate for.

    Returns:
    dict: Funding rate data if successful, None if an error occurs.
    """
    try:
        funding_rate = await exchange.fetch_funding_rate(symbol)
        print(f" {symbol} | {exchange.id}: {funding_rate}")
        return funding_rate
    except Exception as e:
        print(f" {symbol} | {exchange.id}: {e}")
        return None


def construct_okx_symbol(symbol):
    """
    Constructs the symbol format specific to OKX exchange.

    Args:
    symbol (str): The general symbol format (e.g., 'BTC/USDT').

    Returns:
    str: The OKX-specific symbol format (e.g., 'BTC-USDT-SWAP').
    """
    base, quote = symbol.split('/')
    return f"{base}-{quote}-SWAP"


def construct_gate_symbol(symbol):
    """
    Constructs the symbol format specific to Gate.io exchange.

    Args:
    symbol (str): The general symbol format (e.g., 'BTC/USDT').

    Returns:
    str: The Gate.io-specific symbol format (e.g., 'BTC_USDT').
    """
    base, quote = symbol.split('/')
    return f"{base}_{quote}"


def calculate_time_to_funding(funding_timestamp):
    """
    Calculates the time remaining until the next funding event.

    Args:
    funding_timestamp (int): The timestamp of the next funding event in milliseconds.

    Returns:
    str: A string representing the time until funding (e.g., '2hr30min') or 'N/A' if timestamp is None.
    """
    if funding_timestamp is None:
        return "N/A"
    now = datetime.now(timezone.utc).timestamp() * 1000  # current time in milliseconds
    time_diff = funding_timestamp - now
    if time_diff <= 0:
        return "Due"
    hours, remainder = divmod(time_diff / 1000 / 60 / 60, 1)
    minutes = remainder * 60
    return f"{int(hours)}hr{int(minutes)}min"


async def check_funding_rates():
    """
    Checks funding rates across multiple exchanges for configured coins.
    Sends a notification via Telegram if significant rate differences or high rates are found.
    """
    notifications = []
    print(f'Checking Funding Rates - {datetime.now()}')

    for coin in config.ALL_COINS:
        symbol = coin[:-5]  # Remove the last 5 characters
        okx_symbol = construct_okx_symbol(coin)
        binance_symbol = coin
        gate_symbol = construct_gate_symbol(coin)

        # Fetch funding rates from all exchanges concurrently
        okx_data, binance_data, gate_data = await asyncio.gather(
            fetch_funding_rate(okx, okx_symbol),
            fetch_funding_rate(binance, binance_symbol),
            fetch_funding_rate(gate, gate_symbol)
        )

        # Extract funding rates and timestamps
        okx_rate = okx_data['fundingRate'] if okx_data else None
        binance_rate = binance_data['fundingRate'] if binance_data else None
        gate_rate = gate_data['fundingRate'] if gate_data else None

        okx_timestamp = int(okx_data['fundingTimestamp']) if okx_data and 'info' in okx_data and 'nextFundingTime' in \
                                                             okx_data['info'] else None
        binance_timestamp = int(
            binance_data['info']['nextFundingTime']) if binance_data and 'info' in binance_data and 'nextFundingTime' in \
                                                        binance_data['info'] else None
        gate_timestamp = int(gate_data['info'][
                                 'funding_next_apply']) * 1000 if gate_data and 'info' in gate_data and 'funding_next_apply' in \
                                                                  gate_data['info'] else None

        # Calculate time to next funding
        okx_time = calculate_time_to_funding(okx_timestamp)
        binance_time = calculate_time_to_funding(binance_timestamp)
        gate_time = calculate_time_to_funding(gate_timestamp)

        # Check for significant rate differences or high rates
        valid_rates = [rate for rate in [okx_rate, binance_rate, gate_rate] if rate is not None]

        if len(valid_rates) >= 2:
            max_rate = max(valid_rates)
            min_rate = min(valid_rates)
            rate_diff = max_rate - min_rate

            if rate_diff > 0.001 or any(abs(rate) > 0.0015 for rate in valid_rates):
                # Prepare notification message
                notification = f"🚨 {coin}\n"
                notification += f"OKX       : {okx_rate:.3%} | {okx_time}\n" if okx_rate is not None else "OKX       : N/A\n"
                notification += f"Binance : {binance_rate:.3%} | {binance_time}\n" if binance_rate is not None else "Binance : N/A\n"
                notification += f"Gate       : {gate_rate:.3%} | {gate_time}\n" if gate_rate is not None else "Gate       : N/A\n"
                notification += f"https://www.coinglass.com/currencies/{symbol} \n"
                notification += "______________\n\n"
                notifications.append(notification)

    # Prepare and send Telegram message
    if notifications:
        message = "Funding Rate Alert:\n\n" + "".join(notifications)
    else:
        message = "No alerts triggered during this period"

    # Split message if it's too long for a single Telegram message
    max_message_length = 4096
    messages = [message[i:i + max_message_length] for i in range(0, len(message), max_message_length)]

    for msg in messages:
        await bot.send_message(chat_id=CHAT_ID, text=msg)


async def main():
    """
    Main function to run the funding rate checker.
    Sets up a scheduler to run the check every 30 minutes and handles the main event loop.
    """
    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_funding_rates, 'cron', minute='*/30')  # Run every 30 minutes
    scheduler.start()

    try:
        # Run once immediately
        await check_funding_rates()

        # Keep the script running
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        scheduler.shutdown()
        await okx.close()
        await binance.close()
        await gate.close()


if __name__ == "__main__":
    asyncio.run(main())