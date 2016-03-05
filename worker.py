import tasker


class Task(tasker.task.Task):
    name = 'test_task'

    compression = 'none'
    timeout = 30.0
    max_tasks_per_run = 1000
    max_retries = 3

    def init(self):
        self.a = 0

    def work(self, num):
        self.a += num

        return self.a


def main():
    connector = tasker.connectors.redis.Connector(
        host='localhost',
        port=6379,
        database=0,
    )

    task = Task(
        connector=connector,
    )

    worker = tasker.worker.Worker(task, 1, False)
    worker.start()

if __name__ == '__main__':
    try:
        main()
    except:
        print('killed')
