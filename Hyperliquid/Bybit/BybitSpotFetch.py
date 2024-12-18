import ccxt
import asyncio


def fetch_bybit_spot_symbols():
    try:
        # Initialize Bybit client with spot type
        exchange = ccxt.bybit({
            'options': {'defaultType': 'spot'}
        })

        # Load markets
        exchange.load_markets()

        print(exchange.symbols)

        # Get all spot symbols
        symbols = [symbol for symbol in exchange.markets.keys() if symbol.endswith('USDT')]

        # Close the exchange connection

        return sorted(symbols)  # Return sorted list for easier reading

    except Exception as e:
        print(f"Error fetching Bybit spot symbols: {e}")
        return []


# Example usage:
def main():
    symbols = fetch_bybit_spot_symbols()
    print("Available Bybit Spot USDT pairs:")
    for symbol in symbols:
        print(symbol)


if __name__ == "__main__":
    main()