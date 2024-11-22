import logging
from collections import deque
import time
from config import send_telegram_message, bot_tokenHFT, chat_idHFT

class ConsecutiveOrderFilledHandler(logging.Handler):
    def __init__(self, threshold=10, time_window=60):
        super().__init__()
        self.threshold = threshold
        self.time_window = time_window
        self.order_filled_times = deque(maxlen=threshold)

    def emit(self, record):
        if "Order filled" in record.getMessage():
            current_time = time.time()
            self.order_filled_times.append(current_time)

            if len(self.order_filled_times) == self.threshold:
                time_diff = current_time - self.order_filled_times[0]
                if time_diff <= self.time_window:
                    alert_message = f"Alert: {self.threshold} 'Order filled' messages logged within {time_diff:.2f} seconds."
                    send_telegram_message(alert_message, bot_tokenHFT, chat_idHFT)