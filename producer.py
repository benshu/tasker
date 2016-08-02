import consumer
import time


worker = consumer.Worker()

before = time.time()
worker.apply_async_one(num=4)
for i in range(10):
    tasks = []
    for j in range(10000):
        task_obj = worker.craft_task(num=5)
        tasks.append(task_obj)
    worker.apply_async_many(tasks=tasks)
worker.apply_async_one(num=6)
after = time.time()

print(after-before)
