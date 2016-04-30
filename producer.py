import worker
import time


task = worker.Task()

before = time.time()
for i in range(100000):
    task.apply_async(num=6)
after = time.time()

print(after-before)
