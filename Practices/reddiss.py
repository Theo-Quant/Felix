import redis
import time

r = redis.Redis(host='localhost', port=6379)

r.psetex("Germany", 1000, "Berlin")

print(r.get("Germany"))

time.sleep(2)

print(r.get("Germany"))