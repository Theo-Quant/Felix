import ccxt
import redis
import json
import time
import config

okx = ccxt.okx({
    'apiKey': config.OKX_API_KEY,
    'secret': config.OKX_SECRET_KEY,
    'password': config.OKX_PASSPHRASE,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap'
    }
})

# Explicitly set the API credentials
okx.apiKey = config.OKX_API_KEY
okx.secret = config.OKX_SECRET_KEY
okx.password = config.OKX_PASSPHRASE


redis_client = redis.Redis(host='localhost', port=6379, db=0)
logger = config.setup_logger('MaxNotionalCheck')


HFT_COINS = {
    'OM': 7500,
    'MKR': 7500,
    'TIA': 7500,
    'NOT': 7500,
    'ORDI': 7500,
    'MEW': 7500,
    'AAVE': 7500,
    'JTO': 7500
}

ORIGINAL_MAX_POSITIONS = {}  # We'll use this to store original max positions


def check_and_adjust_positions():
    for coin, max_notional in HFT_COINS.items():
        okx_symbol = f"{coin}-USDT-SWAP"

        # Fetch position
        okx_position = okx.fetch_position(okx_symbol)

        # Calculate total notional
        okx_notional = abs(float(okx_position['notional']))

        # Get current parameters
        current_params = get_current_params(coin)
        current_max_position = current_params.get('max_position', 75)
        logger.info(f"Notional for {coin}: {okx_notional}")
        # If we haven't stored the original max position for this coin, do it now
        if coin not in ORIGINAL_MAX_POSITIONS:
            ORIGINAL_MAX_POSITIONS[coin] = current_max_position

        original_max_position = ORIGINAL_MAX_POSITIONS[coin]

        # Check if total notional exceeds max notional
        if okx_notional > max_notional:
            # Calculate new max position
            new_max_position = max(1, int(current_max_position * (max_notional / okx_notional)))

            # Update max position in Redis
            update_params(coin, new_max_position)

            logger.info(f"Adjusted max position for {coin}: {new_max_position}")
        elif okx_notional < max_notional * 0.8:  # If notional is below 80% of max, consider increasing
            # Gradually increase max position, but don't exceed original
            new_max_position = min(int(current_max_position * 1.1), original_max_position)

            if new_max_position > current_max_position:
                update_params(coin, new_max_position)
                logger.info(f"Increased max position for {coin}: {new_max_position}")


def get_current_params(coin):
    params_key = f'bot_params_{coin}'
    params_json = redis_client.get(params_key)
    if params_json:
        return json.loads(params_json)
    return {'max_position': 75}


def update_params(coin, new_max_position):
    params_key = f'bot_params_{coin}'
    params = get_current_params(coin)
    params['max_position'] = new_max_position
    print(f'New max position size for {coin}: {new_max_position}')
    redis_client.set(params_key, json.dumps(params))


def main():
    while True:
        try:
            check_and_adjust_positions()
        except Exception as e:
            print(f"An error occurred: {e}")
        time.sleep(300)  # Check every 5 minutes


if __name__ == "__main__":
    main()