import ccxt
import asyncio
import config

async def place_limit_order():
    # Initialize the Binance exchange
    exchange = ccxt.binance({
        'apiKey': config.BINANCE_API_KEY,
        'secret': config.BINANCE_SECRET_KEY,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'future'
        }
    })

    # Ensure the exchange is initialized
    if exchange.has['fetchTicker'] and exchange.has['createOrder']:
        try:
            # Symbol and amount to trade
            symbol = 'BTC/USDT'
            amount = 0.003  # The amount of BTC to buy

            # Fetch the current market price
            ticker = exchange.fetch_ticker(symbol)
            current_price = ticker['last']

            # Calculate a limit price 0.5% below the current price
            limit_price = current_price * 1.003

            print(f"Current {symbol} price: {current_price}")
            print(f"Placing limit order at: {limit_price}")

            # Place the limit order
            order = exchange.create_limit_sell_order(symbol, amount, limit_price)

            print("Order placed successfully:")
            print(order)

        except ccxt.NetworkError as e:
            print(f"Network error: {str(e)}")
        except ccxt.ExchangeError as e:
            print(f"Exchange error: {str(e)}")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")

    else:
        print("Exchange does not support required methods")

# Run the async function
asyncio.run(place_limit_order())