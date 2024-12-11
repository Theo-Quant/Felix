from pybit.unified_trading import WebSocket
from time import sleep
def handle_message(message, symbol):
    print(symbol)
    print(message)

ws = WebSocket(
    testnet=False,
    channel_type="linear",
)

# Using a lambda to correctly set the callback
ws.orderbook_stream(
    depth=50,
    symbol="BTCUSDT",
    callback=lambda message: handle_message(message, symbol="BTCUSDT"))
while True:
    sleep(1)