import ccxt
import config  # Assuming you have a configGB.py file with API credentials


def cancel_all_okx_orders():
    # Initialize the OKX exchange
    okx = ccxt.okx({
        'apiKey': config.OKX_API_KEY,
        'secret': config.OKX_SECRET_KEY,
        'password': config.OKX_PASSPHRASE,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })

    try:
        # Fetch all open orders
        open_orders = okx.fetch_open_orders()

        # Cancel each open order
        for order in open_orders:
            symbol = order['symbol']
            order_id = order['id']

            try:
                okx.cancel_order(order_id, symbol)
                print(f"Cancelled order {order_id} for {symbol}")
            except Exception as e:
                print(f"Error cancelling order {order_id} for {symbol}: {e}")

        print("All open orders have been cancelled.")

    except Exception as e:
        print(f"An error occurred: {e}")


# Run the function
cancel_all_okx_orders()