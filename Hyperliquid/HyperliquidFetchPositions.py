import ccxt
import config
import asyncio
import time
import aiohttp
from io import StringIO


async def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{config.bot_tokenFunding}/sendMessage"
    data = {
        "chat_id": config.chat_idFunding,
        "text": message
    }
    async with aiohttp.ClientSession() as session:
        await session.post(url, data=data)


def format_positions(data, current_funding_dict):
    # Use StringIO to capture the print outputs
    output = StringIO()

    output.write("Account Summary:\n")
    print(data)
    margin_summary = data['marginSummary']
    maintenance_margin = float(data['crossMaintenanceMarginUsed'])
    output.write(f"Cross Margin Ratio: {maintenance_margin / float(margin_summary['accountValue']) * 100:,.2f}%\n")
    output.write(f"Account Value: ${float(margin_summary['accountValue']):,.2f}\n")
    output.write(f"Total Position Value: ${float(margin_summary['totalNtlPos']):,.2f}\n")
    output.write(f"Margin Used: ${float(margin_summary['totalMarginUsed']):,.2f}\n")
    output.write(f"Withdrawable: ${float(data['withdrawable']):,.2f}\n")
    output.write("\nPosition Details:\n")

    for position in data['assetPositions']:
        pos = position['position']
        size = float(pos['szi'])
        entry = float(pos['entryPx'])
        position_value = float(pos['positionValue'])
        unrealized_pnl = float(pos['unrealizedPnl'])
        liq_price = float(pos['liquidationPx'])
        leverage = pos['leverage']['value']
        funding = pos['cumFunding']['allTime']
        current_funding = float(current_funding_dict[f'{pos['coin']}/USDC:USDC']['info']['funding'])

        output.write(f"Asset: {pos['coin']}\n")
        output.write(f"current_funding: {round(current_funding*100, 5)}% \n")
        output.write(f"Position Value: ${position_value:,.2f}\n")
        output.write(f"Unrealized PnL: ${unrealized_pnl:,.2f}\n")
        output.write(f"Leverage: {leverage}x\n")
        output.write(f"Cumulative Funding: ${-float(funding):,.2f}\n")
        output.write("-" * 25 + "\n")

    print(output.getvalue())
    return output.getvalue()


async def report_positions():
    hyperliquid = ccxt.hyperliquid({
        'apiKey': config.HYPERLIQUID_API_KEY,
        'secret': config.HYPERLIQUID_SECRET_KEY,
        'enableRateLimit': True,
        'walletAddress': config.HYPERLIQUID_MAIN
    })

    while True:
        try:
            hyperliquid.load_markets()
            positions = hyperliquid.fetchBalance({'type': 'futures'})
            data = positions['info']

            funding = hyperliquid.fetch_funding_rates()

            # Format positions and add timestamp
            formatted_positions = format_positions(data, funding)
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S UTC")
            message = f"Position Report ({timestamp})\n\n{formatted_positions}"

            # Send to Telegram
            await send_telegram_message(message)

            # Wait 15 minutes
            await asyncio.sleep(900)  # 15 minutes in seconds

        except Exception as e:
            print(f"Error: {e}")
            await asyncio.sleep(60)  # Wait 1 minute on error before retrying


if __name__ == "__main__":
    asyncio.run(report_positions())