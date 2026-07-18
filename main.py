import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

for key in r.keys("*"):
    key_type = r.type(key)

    if key_type == "string":
        value = r.get(key)
    elif key_type == "hash":
        value = r.hgetall(key)
    elif key_type == "list":
        value = r.lrange(key, 0, -1)
    elif key_type == "set":
        value = list(r.smembers(key))
    elif key_type == "zset":
        value = r.zrange(key, 0, -1, withscores=True)
    else:
        value = "<unsupported type>"

    print(f"{key} ({key_type}) = {value}")
