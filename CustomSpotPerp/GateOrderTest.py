from gate_api import ApiClient, Configuration, SpotApi
from gate_api.exceptions import ApiException, GateApiException
import time
import config


class GateClient:
    def __init__(self, api_key: str, api_secret: str):
        """
        Initialize Gate.io trading client

        Args:
            api_key (str): Your Gate.io API key
            api_secret (str): Your Gate.io API secret
        """
        # Configure API client
        config = Configuration(
            key=api_key,
            secret=api_secret,
            host="https://api.gateio.ws/api/v4"
        )
        self.client = ApiClient(config)
        self.spot_api = SpotApi(self.client)

    def place_market_order(self, symbol: str, side: str, amount: float, price: float):
        """
        Place a market order

        Args:
            symbol (str): Trading pair symbol (e.g. 'BTC_USDT')
            side (str): 'buy' or 'sell'
            amount (float): Amount of base currency to trade

        Returns:
            dict: Order details if successful
        """
        try:
            order = {
                'currency_pair': symbol,
                'type': 'limit',  # Using limit order type
                'side': side,
                'amount': str(amount),  # Always in base currency
                'price': str(price),  # Aggressive price for immediate execution
                'time_in_force': 'ioc',  # Immediate-or-cancel to prevent hanging orders
            }

            print(f"Placing marketable limit order: {order}")
            response = self.spot_api.create_order(order)
            return response

        except GateApiException as ex:
            print(f"Gate.io API Exception: {ex}")
            return None
        except ApiException as e:
            print(f"Exception when calling SpotApi: {e}")
            return None

    def get_account_balance(self, currency: str = None):
        """
        Get account balance for a specific currency or all currencies

        Args:
            currency (str, optional): Currency code (e.g., 'BTC', 'USDT')

        Returns:
            list: Account balances
        """
        try:
            balances = self.spot_api.list_spot_accounts(currency=currency)
            return balances
        except (GateApiException, ApiException) as e:
            print(f"Error getting balance: {e}")
            return None


# Example usage
if __name__ == "__main__":
    # Replace with your API credentials
    API_KEY = config.GATE_API_KEY
    API_SECRET = config.GATE_SECRET_KEY

    # Initialize the client once
    gate = GateClient(API_KEY, API_SECRET)

    # Example: Check USDT balance
    balances = gate.get_account_balance('USDT')
    if balances:
        print(f"USDT Balance: {balances}")

    # Example: Place multiple orders
    # Buy 0.001 BTC
    order1 = gate.place_market_order(
        symbol="MOODENG_USDT",
        side="buy",
        amount=10,
        price=0.3
    )

    if order1:
        print(f"BTC order placed successfully: {order1}")
