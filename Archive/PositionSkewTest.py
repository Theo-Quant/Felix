import redis
import json
import math

host='localhost'
port=6379
db=0
redis_client = redis.Redis(host=host, port=port, db=db)

# b'{"notional_per_trade": 400, "ma": 10, "max_notional": 253468.7936318016, "default_max_notional": 150000,
# "std_coeff": 1.2, "min_width": 0.065, "mark_price": 3.7782e-05, "position_size": 266809.256454528}'
position_data = redis_client.get('bot_params_HMSTR')
position_data_dict = json.loads(position_data)

# {'sell_spread_ma_M': -0.0414826131, 'buy_spread_ma_M': 0.0117816999, 'sell_spread_sd_M': 0.0222432919,
# 'buy_spread_sd_M': 0.0223228349, 'sell_spread_ma_L': -0.0507289017, 'buy_spread_ma_L': 0.002247195,
# 'sell_spread_sd_L': 0.0178388247, 'buy_spread_sd_L': 0.0183385944, 'current_sell_spread': -0.0523834468,
# 'current_buy_spread': 0.0}
trend_data = redis_client.hget('trend_data', 'HMSTR/USDT')
coin_data = json.loads(trend_data)

print(position_data_dict)
print(coin_data)

sell_bound = round(max(
    (coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M']) / 2 + coin_data['sell_spread_sd_L'] * 1.2,
    0.065 / 2 + (coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M']) / 2), 4)
buy_bound = round(min(
    (coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M']) / 2 - coin_data['buy_spread_sd_L'] * 1.2,
    (coin_data['buy_spread_ma_M'] + coin_data['sell_spread_ma_M']) / 2 - 0.065 / 2), 4)

current_position = float(position_data_dict['position_size'])
max_position = 300000

capacity = round(current_position / max_position, 3)  # This naturally gives us [-1, 1]
print(f'{capacity*100}% to max capacity')

print('Sell Bound: ', sell_bound, 'Buy Bound: ', buy_bound)

def calculate_skew(capacity, max_skew=0.1):
   """
   Calculate position skew with exponential increase near edges
   :param capacity: Position capacity in range [-1, 1]
   :param max_skew: Maximum skew amount
   :return: Skew amount
   """
   # Option 1: Square function (x²) - moderate edge severity
   return -math.copysign(1, capacity) * (capacity * capacity) * max_skew

   # Option 2: Cube function (x³) - more edge severity
   # return -(capacity ** 3) * max_skew

   # Option 3: Fourth power (x⁴) - even more edge severity
   # return -math.copysign(1, capacity) * (capacity ** 4) * max_skew


# skew_amount = calculate_skew(capacity)
#
# print(f'Inventory at {position_data_dict['position_size']}, % to max capacity : {capacity}% | Inventory Skew: {round(skew_amount,4)}%')
#
# sell_bound_skew = round(sell_bound + skew_amount, 4)
# buy_bound_skew = round(buy_bound + skew_amount, 4)
#
# print('Sell Bound: ', sell_bound_skew, 'Buy Bound: ', buy_bound_skew)


# Generate positions from -1 to 1 in steps of 0.05
positions = [i/20 for i in range(-20, 21)]  # -1.0 to 1.0 in steps of 0.05

print("\nCapacity | Linear  | Square  | Cube    | Fourth")
print("-" * 50)
for pos in positions:
   linear = -pos * 0.1
   square = -math.copysign(1, pos) * (pos * pos) * 0.1
   cube = -(pos ** 3) * 0.1
   fourth = -math.copysign(1, pos) * (pos ** 4) * 0.1
   print(f"{pos:7.2f} | {linear:7.4f} | {square:7.4f} | {cube:7.4f} | {fourth:7.4f}")


