import tasker
import worker
import datetime
import time


connector = tasker.connectors.redis.Connector(
    host='localhost',
    port=6379,
    database=0,
)
monitor_client = tasker.monitor.client.StatisticsClient(
    stats_server={
        'host': '127.0.0.1',
        'port': 9999,
    },
    host_name='test_host',
    worker_name='test_worker',
)
task_queue = tasker.queue.Queue(
    connector=connector,
    queue_name='test_task',
    compressor='none',
    serializer='msgpack',
)
task = worker.Task(
    task_queue=task_queue,
    monitor_client=monitor_client,
)

scheduler = tasker.scheduler.Scheduler()
scheduler.start()
# scheduler.run_every(
#     task=task,
#     args=[],
#     kwargs={
#         'num': 5,
#     },
#     time_delta=datetime.timedelta(seconds=3),
# )


before = time.time()
for i in range(100000):
    scheduler.run_now(task, args=[], kwargs={'num': 5})
scheduler.run_now(task, args=[], kwargs={'num': 6})
after = time.time()

print(after-before)
time.sleep(10)
scheduler.terminate()
