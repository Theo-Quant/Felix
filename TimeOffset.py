import time
import requests
import threading

class TimeSync:
    def __init__(self, interval=300):
        """
        Initializes the TimeSync class.

        Parameters:
        interval (int): The interval (in seconds) to fetch the server time and update the offset. Default is 300 seconds (5 minutes).
        """
        self.interval = interval
        self.offset = 0
        self.update_offset()
        self.start()

    def get_binance_server_time(self):
        """
        Fetches the current server time from Binance.

        Returns:
        int: The server time in milliseconds.
        """
        response = requests.get('https://api.binance.com/api/v3/time')
        server_time = response.json()['serverTime']
        return server_time

    def calculate_time_offset(self):
        """
        Calculates the time offset between the local machine and Binance server.

        Returns:
        int: The time offset in milliseconds.
        """
        server_time = self.get_binance_server_time()
        local_time = int(time.time() * 1000)
        offset = server_time - local_time
        return offset

    def update_offset(self):
        """
        Updates the time offset by calculating the current offset.
        """
        self.offset = self.calculate_time_offset()
        print(f"Time offset updated: {self.offset} ms")

    def start(self):
        """
        Starts the periodic update of the time offset.
        """
        self.update_offset()
        self.timer = threading.Timer(self.interval, self.start)
        self.timer.start()

    def stop(self):
        """
        Stops the periodic update of the time offset.
        """
        if self.timer.is_alive():
            self.timer.cancel()

    def get_adjusted_time(self):
        """
        Gets the current local time adjusted by the offset.

        Returns:
        int: The adjusted time in milliseconds.
        """
        local_time = int(time.time() * 1000)
        adjusted_time = local_time + self.offset
        return adjusted_time

# Usage example
if __name__ == "__main__":
    time_sync = TimeSync(interval=300)
    try:
        while True:
            adjusted_time = time_sync.get_adjusted_time()
            print(f"Adjusted time: {adjusted_time}")
            time.sleep(10)
    except KeyboardInterrupt:
        time_sync.stop()
