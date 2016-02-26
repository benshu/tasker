import tasker
import worker
import time


connector = tasker.connectors.redis.Connector(
    host='localhost',
    port=6379,
    database=0,
)

task = worker.Task(
    connector=connector,
)

before = time.time()
for i in range(10000):
    task.run(num=5)
after = time.time()

print(after-before)
