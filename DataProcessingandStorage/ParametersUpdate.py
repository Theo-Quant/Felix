import redis
import json

class BotParamManager:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = redis.Redis(host=host, port=port, db=db)

    def get_bot_params(self, bot_id):
        """Retrieve the current parameters for a specific bot."""
        params_key = f'bot_params_{bot_id}'
        params_json = self.redis_client.get(params_key)
        if params_json:
            return json.loads(params_json)
        else:
            return None

    def update_bot_params(self, bot_id, **params):
        """Update parameters for a specific bot."""
        params_key = f'bot_params_{bot_id}'
        current_params = self.get_bot_params(bot_id) or {}
        current_params.update(params)
        self.redis_client.set(params_key, json.dumps(current_params))
        print(f"Updated parameters for bot {bot_id}: {params}")

    def delete_bot_params(self, bot_id):
        """Delete all parameters for a specific bot."""
        params_key = f'bot_params_{bot_id}'
        result = self.redis_client.delete(params_key)
        if result:
            print(f"Deleted all parameters for bot {bot_id}")
        else:
            print(f"No parameters found for bot {bot_id}")

    def get_all_bot_params(self):
        """Retrieve and display parameters for all bots."""
        all_keys = self.redis_client.keys('bot_params_*')
        all_params = {}
        for key in all_keys:
            bot_id = key.decode('utf-8').split('_')[-1]
            params = self.get_bot_params(bot_id)
            if params:
                all_params[bot_id] = params
        return all_params


def display_all_params(all_params):
    """Display parameters for all bots in a formatted way."""
    if all_params:
        for bot_id, params in all_params.items():
            print(f"\nParameters for bot {bot_id}:")
            for key, value in params.items():
                print(f"  {key}: {value}")
    else:
        print("No parameters found for any bots.")


# Example usage
if __name__ == "__main__":
    manager = BotParamManager()
    # Get parameters
    # params = manager.get_bot_params(bot_id)
    # if params:
    #     print(f"Current parameters for bot {bot_id}:")
    #     for key, value in params.items():
    #         print(f"  {key}: {value}")
    # else:
    #     print(f"No parameters found for bot {bot_id}")

    # Update parameters
    """
    max_notional: Actual max notional per coin, this will be changed by the PositionUpload bot based on how much total position size there is.
    default_max_notional: This will be used by the PositionUpload bot to set specific max notionals when the total position size isn't maxed out.
    ma: Number of rows to pull from the redis to calculate MA.
    notional_per_trade: The USDT amount of notional to be placed for each order.
    std_coeff: The coefficient of the std used in the bound calculation.
    min_width: The minimum width of the buy/sell bounds. The value is in %. 0.05 would be 5 bps.
    max_skew: The maximum shift the skew function can impact the buy/sell bounds. The value is in %. 0.05 would be 5 bps.
    """

    # Regular Bots
    manager.update_bot_params("MOODENG", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=150000, std_coeff=1.2, min_width=0.065, max_skew=0.04)
    manager.update_bot_params("MASK", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("NEIRO", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("APE", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("POPCAT", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("CAT", notional_per_trade=300, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("HMSTR", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=0, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("BIGTIME", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("AGLD", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=175000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("UXLINK", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("TNSR", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.4, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("RDNT", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=0, std_coeff=1.2, min_width=0.06, max_skew=0.04) # OKX has to be 5X
    manager.update_bot_params("LQTY", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=200000, std_coeff=1.4, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("DOGS", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("ENJ", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=125000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("TRX", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("ZETA", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.075, max_skew=0.04)
    manager.update_bot_params("SUI", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.3, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("OM", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=80000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("MKR", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.4, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("TIA", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("AAVE", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.4, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("JTO", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("MEW", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.3, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("STORJ", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=0, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("TURBO", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.4, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("AR", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("TRB", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.3, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("AUCTION", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=250000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("ORBS", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=80000, std_coeff=1.2, min_width=0.06, max_skew=0.04)
    manager.update_bot_params("ALPHA", notional_per_trade=250, ma=10, max_notional=20000, default_max_notional=175000, std_coeff=1.2, min_width=0.06, max_skew=0.04)

    # Bots on Pause
    manager.update_bot_params("NOT", notional_per_trade=200, ma=10, max_notional=20000, default_max_notional=1000, std_coeff=1.2, min_width=0.06, max_skew=0.02)
    manager.update_bot_params("TON", notional_per_trade=200, ma=10, max_notional=20000, default_max_notional=1000, std_coeff=1.2, min_width=0.06, max_skew=0.02)
    manager.update_bot_params("BNT", notional_per_trade=200, ma=10, max_notional=20000, default_max_notional=1000, std_coeff=1.2, min_width=0.06, max_skew=0.02)
    manager.update_bot_params("ORDI", notional_per_trade=200, ma=10, max_notional=20000, default_max_notional=1000, std_coeff=1.2, min_width=0.07, max_skew=0.02)
    manager.update_bot_params("UMA", notional_per_trade=200, ma=10, max_notional=20000, default_max_notional=1000, std_coeff=1.2, min_width=0.055, max_skew=0.02)


    # Delete parameters
    # manager.delete_bot_params("coin_name i.e BTC")



    # Get and display parameters for all bots
    print("\nDisplaying parameters for all bots:")
    all_params = manager.get_all_bot_params()
    display_all_params(all_params)