import redis

"""
Code used to manually reset any flags associated with the bot.
"""

r = redis.Redis(host='localhost', port=6379, db=0)

# when value false it runs, if it's true it stops
r.set('stop_bot', 'false')
print(r.get('stop_bot'))

# when value is 0, it causes the bot to only initiate exit trades, this is set when margin balance reaches 0
r.set('only_exit', 1)
print(r.get('only_exit'))
