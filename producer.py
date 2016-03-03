import tasker
import worker
import datetime
import time


connector = tasker.connectors.redis.Connector(
    host='localhost',
    port=6379,
    database=0,
)

task = worker.Task(
    connector=connector,
)

scheduler = tasker.scheduler.Scheduler()
scheduler.start()
# scheduler.run_every(
#     time_delta=datetime.timedelta(seconds=3),
#     task=task,
#     args=[],
#     kwargs={
#         'num': 5,
#     },
# )


before = time.time()
for i in range(100000000):
    scheduler.run_now(task, args=[], kwargs={'num':5})
after = time.time()

print(after-before)
