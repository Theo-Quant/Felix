from pybit.unified_trading import WebSocket
from time import sleep
ws = WebSocket(
    testnet =False,
    channel_type="linear",
)
def handle_message(message):
    print(message)
ws.orderbook_stream(
    depth=50,
    symbol="BTCUSDT",
    callback=handle_message
)
while True:
    sleep(1)