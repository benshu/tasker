import tasker
import logging


class Task(tasker.task.Task):
    name = 'test_task'

    queue = {
        'type': 'regular',
        'name': 'test_task',
    }
    compressor = 'dummy'
    serializer = 'pickle'
    monitoring = {
        'host_name': '',
        'stats_server': {
            'host': '127.0.0.1',
            'port': 9999,
        }
    }
    connector = {
        'type': 'redis',
        'params': {
            'host': 'localhost',
            'port': 6379,
            'database': 0,
        },
    }
    timeout = 30.0
    max_tasks_per_run = 100000
    tasks_per_transaction = 1000
    max_retries = 3
    log_level = logging.ERROR
    report_completion = True

    def init(self):
        self.a = 0

    def work(self, num):
        self.a += num


def main():
    worker = tasker.worker.Worker(
        task_class=Task,
        concurrent_workers=4,
    )
    worker.log_level = logging.ERROR
    worker.start()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
        print('killed')
