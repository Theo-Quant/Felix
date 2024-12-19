from dotenv import load_dotenv
import os
import telebot
import time
import requests

# Load .env file
load_dotenv()
API_TOKEN= os.getenv('BOT_TOKEN', "")
bot = telebot.TeleBot(API_TOKEN)
CHAT_ID ='7346012683'
def fetch_data():
    # Example URLs (replace with actual API endpoints)
    funding_rate_url = 'https://api.bybit.com/v2/public/funding/prev'
    spread_url = 'https://api.hyperliquid.com/v1/spreads'

    funding_response = requests.get(funding_rate_url).json()
    spread_response = requests.get(spread_url).json()

    # Extract necessary data (modify as per actual API response structure)
    return funding_response['result'], spread_response['result']


# Function to check for alerts
def check_alerts(threshold):
    funding_data, spread_data = fetch_data()

    for data in funding_data:
        funding_rate = float(data['funding_rate'])
        coin = data['symbol']

        # Check if funding rate exceeds the threshold
        if funding_rate > threshold:
            alert_message = f"Funding rate alert for {coin}: {funding_rate * 100:.2f}%"
            bot.send_message(CHAT_ID, alert_message)

    for data in spread_data:
        spread = float(data['spread'])
        coin = data['symbol']

        # Check if spread exceeds the threshold
        if spread > threshold:
            alert_message = f"Spread alert for {coin}: {spread * 100:.2f}%"
            bot.send_message(CHAT_ID, alert_message)


# Main loop to continuously check for alerts
def main():
    threshold = 0.02  # Example threshold (2%)
    while True:
        check_alerts(threshold)
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    main()