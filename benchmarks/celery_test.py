from celery import Celery
import time

app = Celery('tasks', broker='redis://127.0.0.1/')


@app.task(bind=True, name='tasks.add')
def add(self, a):
    self.sum = a
    if a == 6:
        print(time.time())
print(time.time())

if __name__ == '__main__':
    before = time.time()
    for i in range(10000):
        add.delay(a=5)
    add.delay(a=6)
    after = time.time()

    print(after-before)
