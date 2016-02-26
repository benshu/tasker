from celery import Celery

app = Celery('tasks', broker='redis://127.0.0.1/')


@app.task(bind=True)
def add(self, a):
    if hasattr(self, 'sum'):
        self.sum += a
    else:
        self.sum = a
    print(self.sum)
