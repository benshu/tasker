import tasker


class Task(tasker.task.Task):
    name = 'test_task'

    compression = 'none'
    timeout = 30.0
    max_tasks_per_run = 100
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

    worker = tasker.worker.Worker(task, 4, True)
    worker.start()

if __name__ == '__main__':
    main()
