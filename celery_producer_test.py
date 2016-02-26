from celery import Celery
import time

app = Celery('tasks', broker='redis://127.0.0.1/')


@app.task(bind=True)
def add(self, a):
    if hasattr(self, 'sum'):
        self.sum += a
    else:
        self.sum = a
    print(self.sum)

before = time.time()
for i in range(10000):
    add.delay(a=5)
after = time.time()

print(after-before)
