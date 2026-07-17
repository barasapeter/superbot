import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# r.set("name", "Peter")

r.delete("name")
print(r.get("name"))
