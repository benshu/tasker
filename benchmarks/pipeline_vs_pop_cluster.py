import time
import rediscluster


conn = rediscluster.StrictRedisCluster(
    startup_nodes=[
        {
            'host': '127.0.0.1',
            'port': 6379,
        },
        {
            'host': '127.0.0.1',
            'port': 6380,
        },
        {
            'host': '127.0.0.1',
            'port': 6381,
        },
    ],
    retry_on_timeout=True,
    socket_keepalive=False,
    socket_connect_timeout=10,
    socket_timeout=60,
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
