import ccxt
import asyncio
import config

def place_limit_order():
    # Initialize the OKX exchange
    exchange = ccxt.okx({
        'apiKey': config.OKX_API_KEY,
        'secret': config.OKX_SECRET_KEY,
        'password': config.OKX_PASSPHRASE,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap'  # OKX uses 'swap' for futures
        }
    })

    # Ensure the exchange is initialized
    if exchange.has['fetchTicker'] and exchange.has['createOrder']:
        try:
            # Symbol and amount to trade
            symbol = 'ETH/USDT:USDT'  # OKX uses this format for USDT-margined futures
            amount = 0.1  # The amount of ETH to buy

            # Fetch the current market price
            ticker = exchange.fetch_ticker(symbol)
            current_price = ticker['last']

            # Calculate a limit price 0.5% above the current price
            limit_price = round(current_price * 1.005, 1)

            print(f"Current {symbol} price: {current_price}")
            print(f"Placing limit order at: {limit_price}")

            # Place the limit order
            # Place the reduce-only limit order
            order = exchange.create_order(
                symbol,
                type='market',
                side='sell',
                amount=amount,
                price=limit_price,
                params={
                    'reduceOnly': False
                }
            )
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
place_limit_order()