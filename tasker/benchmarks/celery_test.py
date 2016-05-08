from celery import Celery
import time

app = Celery('tasks', broker='redis://127.0.0.1/')
app.conf.update(
    CELERY_DISABLE_RATE_LIMITS=True,
    BROKER_POOL_LIMIT=10,
    CELERYD_PREFETCH_MULTIPLIER=1000,
    CELERY_MESSAGE_COMPRESSION=None,
    CELERY_TASK_SERIALIZER='pickle',
    CELERY_ACCEPT_CONTENT=['pickle'],
    BROKER_CONNECTION_RETRY=True,
    BROKER_CONNECTION_MAX_RETRIES=0,
)

@app.task(bind=True, name='tasks.add')
def add(self, a):
    self.sum = a
    if a == 4:
        print(time.time())
    if a == 6:
        print(time.time())

if __name__ == '__main__':
    before = time.time()
    add.delay(a=4)
    for i in range(100000):
        add.delay(a=5)
    add.delay(a=6)
    after = time.time()

    print(after-before)
