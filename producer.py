import worker
import time


task = worker.Task()

before = time.time()
for i in range(100):
    tasks = []
    for j in range(1000):
        task_obj = task.craft_task(num=6)
        tasks.append(task_obj)
    task.apply_async_many(tasks=tasks)
after = time.time()

print(after-before)
