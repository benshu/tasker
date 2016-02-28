import tasker
import worker
import datetime


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
scheduler.run_within(
    task=task,
    time_delta=datetime.timedelta(seconds=10),
    args=[],
    kwargs={
        'num': 5,
    },
)


# before = time.time()
# for i in range(10000):
#     task.run(num=5)
# after = time.time()
#
# print(after-before)
