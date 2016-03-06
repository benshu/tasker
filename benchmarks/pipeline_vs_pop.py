import time
import redis


conn = redis.StrictRedis(
    host='127.0.0.1',
    port=6379,
    db=0,
)

before = time.time()
for i in range(100000):
    conn.rpush('test', 'abcdefghijklmnopqrstuvwxyz0123456789')
after = time.time()

print(
    'simple insertion: {time_taken}'.format(
        time_taken=after-before,
    )
)

before = time.time()
for i in range(100000):
    conn.blpop(
        keys=['test'],
        timeout=0,
    )
after = time.time()

print(
    'simple: {time_taken}'.format(
        time_taken=after-before,
    )
)

before = time.time()
for i in range(100):
    pipe = conn.pipeline()
    for j in range(1000):
        pipe.rpush('test', 'abcdefghijklmnopqrstuvwxyz0123456789')
    pipe.execute()
after = time.time()

print(
    'pipeline insertion: {time_taken}'.format(
        time_taken=after-before,
    )
)

before = time.time()
for i in range(100):
    pipe = conn.pipeline()
    pipe.lrange('test', 0, 999)
    pipe.ltrim('test', 0, 999)
    pipe.execute()
after = time.time()

print(
    'pipeline: {time_taken}'.format(
        time_taken=after-before,
    )
)
