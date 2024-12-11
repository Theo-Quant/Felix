import redis
import json

class BotParamManager:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = redis.Redis(host=host, port=port, db=db)

    def get_bot_params(self, bot_id):
        """Retrieve the current parameters for a specific bot."""
        params_key = f'Perp_Perp_bot_params_{bot_id}'
        params_json = self.redis_client.get(params_key)
        if params_json:
            return json.loads(params_json)
        else:
            return None

    def update_bot_params(self, bot_id, **params):
        """Update parameters for a specific bot."""
        params_key = f'Perp_Perp_bot_params_{bot_id}'
        current_params = self.get_bot_params(bot_id) or {}
        current_params.update(params)
        self.redis_client.set(params_key, json.dumps(current_params))
        print(f"Updated parameters for bot {bot_id}: {params}")

    def delete_bot_params(self, bot_id):
        """Delete all parameters for a specific bot."""
        params_key = f'Perp_Perp_bot_params_{bot_id}'
        result = self.redis_client.delete(params_key)
        if result:
            print(f"Deleted all parameters for bot {bot_id}")
        else:
            print(f"No parameters found for bot {bot_id}")

    def get_all_bot_params(self):
        """Retrieve and display parameters for all bots."""
        all_keys = self.redis_client.keys('Perp_Perp_bot_params_*')
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
    """

    # Perp Perps
    manager.update_bot_params("CTC", notional_per_trade=50, ma=10, max_notional=250, default_max_notional=250, std_coeff=1, min_width=0.065)
    manager.update_bot_params("FTM", notional_per_trade=50, ma=10, max_notional=250, default_max_notional=250, std_coeff=1, min_width=0.065)
    manager.update_bot_params("DOGE", notional_per_trade=50, ma=10, max_notional=250, default_max_notional=250, std_coeff=1, min_width=0.065)
    manager.update_bot_params("DOGS", notional_per_trade=50, ma=10, max_notional=250, default_max_notional=250, std_coeff=1, min_width=0.065)
    manager.update_bot_params("XRP", notional_per_trade=50, ma=10, max_notional=250, default_max_notional=250, std_coeff=1, min_width=0.065)

    # Delete parameters
    # manager.delete_bot_params('SUI')

    # Get and display parameters for all bots
    print("\nDisplaying parameters for all bots:")
    all_params = manager.get_all_bot_params()
    display_all_params(all_params)