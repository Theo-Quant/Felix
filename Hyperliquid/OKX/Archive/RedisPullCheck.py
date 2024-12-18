import redis
import json
from datetime import datetime
import time
import pandas as pd


class OrderbookDataReader:
    def __init__(self, host='localhost', port=6379, db=0):
        """Initialize Redis connection"""
        self.redis_client = redis.Redis(host=host, port=port, db=db)

    def get_latest_data(self, coin, n_entries=500):
        """
        Get the latest n entries for a specific coin
        Returns a pandas DataFrame with the data
        """
        key = f'HyperliquidOKX_combined_data_{coin}'

        # Get all entries (up to n_entries)
        data = self.redis_client.lrange(key, -n_entries, -1)

        if not data:
            print(f"No data found for {coin}")
            return None

        # Parse JSON data
        parsed_data = [json.loads(entry) for entry in data]

        # Convert to DataFrame
        df = pd.DataFrame(parsed_data)

        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

    def get_current_spreads(self, coin):
        """Get the most recent spread data for a specific coin"""
        df = self.get_latest_data(coin, n_entries=1)

        if df is None or df.empty:
            return None

        latest = df.iloc[-1]
        return {
            'timestamp': latest['timestamp'],
            'entry_spread': latest['entry_spread'],
            'exit_spread': latest['exit_spread'],
            'timelag': latest['timelag']
        }

    def get_orderbook_snapshot(self, coin):
        """Get the most recent complete orderbook for both exchanges"""
        df = self.get_latest_data(coin, n_entries=1)

        if df is None or df.empty:
            return None

        latest = df.iloc[-1]
        return {
            'timestamp': latest['timestamp'],
            'okx_orderbook': latest['okx_orderbook'],
            'hyperliquid_orderbook': latest['hyperliquid_orderbook']
        }

    def monitor_spreads(self, coin, threshold=0.5, interval=1):
        """
        Monitor spreads in real-time and print when they exceed the threshold
        threshold: minimum spread percentage to trigger alert
        interval: how often to check (in seconds)
        """
        print(f"Monitoring {coin} spreads. Press Ctrl+C to stop...")
        try:
            while True:
                spreads = self.get_current_spreads(coin)
                if spreads:
                    if abs(spreads['entry_spread']) > threshold or abs(spreads['exit_spread']) > threshold:
                        print(f"\nSignificant spread detected at {spreads['timestamp']}:")
                        print(f"Entry Spread: {spreads['entry_spread']}%")
                        print(f"Exit Spread: {spreads['exit_spread']}%")
                        print(f"Time Lag: {spreads['timelag']}ms")

                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nStopped monitoring.")


def main():
    # Example usage
    reader = OrderbookDataReader()

    # List of coins to monitor (matching the WebSocket script)
    coins = ["HYPE"]

    for coin in coins:
        print(f"\nAnalyzing {coin} data:")

        # Get latest spreads
        spreads = reader.get_current_spreads(coin)
        if spreads:
            print(f"Latest spreads at {spreads['timestamp']}:")
            print(f"Entry Spread: {spreads['entry_spread']}%")
            print(f"Exit Spread: {spreads['exit_spread']}%")
            print(f"Time Lag: {spreads['timelag']}ms")

        # Get historical data
        df = reader.get_latest_data(coin)
        if df is not None:
            print(f"\nHistorical Statistics:")
            print(f"Average Entry Spread: {df['entry_spread'].mean():.4f}%")
            print(f"Average Exit Spread: {df['exit_spread'].mean():.4f}%")
            print(f"Max Entry Spread: {df['entry_spread'].max():.4f}%")
            print(f"Max Exit Spread: {df['exit_spread'].max():.4f}%")

        # Option to monitor spreads continuously
        monitor = input("\nWould you like to monitor spreads in real-time? (y/n): ")
        if monitor.lower() == 'y':
            reader.monitor_spreads(coin, threshold=0.5)


if __name__ == "__main__":
    main()