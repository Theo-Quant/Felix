"""
BinanceActivationPings.py is a Python script designed to maintain an active connection with the Binance futures trading platform. It does this by periodically placing and replacing small "ping" orders. Here's a breakdown of its operation:

The script defines a PingService class that handles the ping functionality.
When initialized, the PingService takes an AsyncClient object, which is an asynchronous client for interacting with the Binance API.
The main function of the class is keep_alive_ping, which runs continuously in an infinite loop:

If there's no active ping order, it places a new one.
If there's an existing ping order, it cancels and replaces it with a new one.
If anything fails, it changes the ping_order_id = None, which sets the script to send in a new ping on the next iteration.
Any errors will be logged.
This process repeats every 15 seconds.


The ping orders are small, low-value orders (0.01 BNB at $400) that are unlikely to be filled. They're not meant for actual trading, just for keeping the connection active.
If any errors occur during this process (e.g., network issues, API problems), the script catches these exceptions, logs them, and tries again after a short delay.
The script uses asyncio for asynchronous operation, allowing it to handle network operations efficiently without blocking.
When run as a standalone script, it creates an AsyncClient, initializes the PingService, and starts the keep_alive_ping process for the BNB trading pair.
The script ensures proper cleanup by closing the client connection when it's done or if an error occurs.

The purpose of this "ping" mechanism is to prevent the trading connection from timing out due to inactivity. By regularly sending these small orders, it ensures that the connection to Binance remains open and responsive, which is crucial for high-frequency trading strategies where every millisecond counts.
This script is designed to be run alongside other components of a trading system, keeping the connection alive while other parts of the system handle actual trading operations.
"""

import time
from binance.client import Client as BinanceClient
import config
from TimeOffset import TimeSync
import threading

logger = config.setup_logger('BinanceActivationPings')


class PingService:
    def __init__(self, client: BinanceClient, time_sync: TimeSync):
        self.client = client
        self.ping_order_id = None
        self.time_sync = time_sync
        self.running = True

    def keep_alive_ping(self, symbol, interval=15):
        while self.running:
            try:
                if self.ping_order_id is None:
                    self.ping_order_id = self.place_ping_order(symbol)
                    print(f"Ping order starting - {self.ping_order_id}")
                else:
                    self.ping_order_id = self.cancel_and_replace_ping_order(symbol, self.ping_order_id)
                    print(f"Ping order updated - {self.ping_order_id}")

                time.sleep(interval)
            except Exception as e:
                logger.error(f"Error in keep_alive_ping: {e}")
                self.ping_order_id = None
                time.sleep(5)

    def place_ping_order(self, symbol):
        try:
            adjusted_timestamp = str(int(self.time_sync.get_adjusted_time() / 1000))
            order = self.client.futures_create_order(
                symbol=symbol,
                side='BUY',
                type='LIMIT',
                quantity=0.02,
                price='400',
                timeInForce='GTC',
                timestamp=adjusted_timestamp
            )
            return order['orderId']
        except Exception as e:
            logger.error(f"Failed to place ping order: {e}")
            return None

    def cancel_and_replace_ping_order(self, symbol, order_id):
        try:
            adjusted_timestamp = str(int(self.time_sync.get_adjusted_time() / 1000))
            self.client.futures_cancel_order(symbol=symbol, orderId=order_id, timestamp=adjusted_timestamp)
            return self.place_ping_order(symbol)
        except Exception as e:
            logger.error(f"Failed to cancel and replace ping order: {e}")
            return None

    def start(self, symbol='BNBUSDT', interval=15):
        self.ping_thread = threading.Thread(target=self.keep_alive_ping, args=(symbol, interval))
        self.ping_thread.start()

    def stop(self):
        self.running = False
        if hasattr(self, 'ping_thread'):
            self.ping_thread.join()


def run_ping_service(binance_client, time_sync):
    ping_service = PingService(binance_client, time_sync)
    ping_thread = threading.Thread(target=ping_service.keep_alive_ping, args=('BNBUSDT',))
    ping_thread.start()
    return ping_service, ping_thread


if __name__ == "__main__":
    binance_client = BinanceClient(config.BINANCE_API_KEY, config.BINANCE_SECRET_KEY)
    time_sync = TimeSync(interval=60)
    ping_service, ping_thread = run_ping_service(binance_client, time_sync)

    try:
        ping_thread.join()
    except KeyboardInterrupt:
        print("Stopping ping service...")
        ping_service.stop()
        ping_thread.join()
    finally:
        binance_client.close_connection()