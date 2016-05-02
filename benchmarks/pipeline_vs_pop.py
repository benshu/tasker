import time
import redis


conn = redis.StrictRedis(
    host='127.0.0.1',
    port=6379,
    db=0,
)

before = time.time()
for i in range(100000):
    conn.rpush('a', 'abcdefghijklmnopqrstuvwxyz0123456789')
    conn.rpush('b', 'abcdefghijklmnopqrstuvwxyz0123456789')
    conn.rpush('c', 'abcdefghijklmnopqrstuvwxyz0123456789')
after = time.time()

print(
    'simple insertion: {time_taken}'.format(
        time_taken=after-before,
    )
)

before = time.time()
for i in range(100000):
    conn.blpop(
        keys=['a'],
        timeout=0,
    )
    conn.blpop(
        keys=['b'],
        timeout=0,
    )
    conn.blpop(
        keys=['c'],
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
        pipe.rpush('a', 'abcdefghijklmnopqrstuvwxyz0123456789')
        pipe.rpush('b', 'abcdefghijklmnopqrstuvwxyz0123456789')
        pipe.rpush('c', 'abcdefghijklmnopqrstuvwxyz0123456789')
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
    pipe.lrange('a', 0, 999)
    pipe.ltrim('a', 0, 999)
    pipe.lrange('b', 0, 999)
    pipe.ltrim('b', 0, 999)
    pipe.lrange('c', 0, 999)
    pipe.ltrim('c', 0, 999)
    pipe.execute()
after = time.time()

print(
    'pipeline: {time_taken}'.format(
        time_taken=after-before,
    )
)

before = time.time()
for i in range(100):
    pipe = conn.pipeline()
    for j in range(1000):
        pipe.rpush('a', 'abcdefghijklmnopqrstuvwxyz0123456789')
        pipe.rpush('b', 'abcdefghijklmnopqrstuvwxyz0123456789')
        pipe.rpush('c', 'abcdefghijklmnopqrstuvwxyz0123456789')
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
    for j in range(1000):
        pipe.lpop('a')
        pipe.lpop('b')
        pipe.lpop('c')
    pipe.execute()
after = time.time()

print(
    'pipeline lpop: {time_taken}'.format(
        time_taken=after-before,
    )
)

before = time.time()
for i in range(100):
    pipe = conn.pipeline()
    arr = ['abcdefghijklmnopqrstuvwxyz0123456789'] * 1000
    pipe.rpush('a', *arr)
    pipe.rpush('b', *arr)
    pipe.rpush('c', *arr)
    pipe.execute()
after = time.time()

print(
    'pipeline multiple rpush insertion: {time_taken}'.format(
        time_taken=after-before,
    )
)

before = time.time()
for i in range(100):
    pipe = conn.pipeline()
    for j in range(1000):
        pipe.lpop('a')
        pipe.lpop('b')
        pipe.lpop('c')
    pipe.execute()
after = time.time()

print(
    'pipeline lpop: {time_taken}'.format(
        time_taken=after-before,
    )
)

before = time.time()
arr = ['abcdefghijklmnopqrstuvwxyz0123456789'] * 100000
conn.rpush('a', *arr)
conn.rpush('b', *arr)
conn.rpush('c', *arr)
after = time.time()

print(
    'multiple rpush insertion: {time_taken}'.format(
        time_taken=after-before,
    )
)

before = time.time()
for i in range(100):
    pipe = conn.pipeline()
    for j in range(1000):
        pipe.lpop('a')
        pipe.lpop('b')
        pipe.lpop('c')
    pipe.execute()
after = time.time()

print(
    'pipeline lpop: {time_taken}'.format(
        time_taken=after-before,
    )
)
