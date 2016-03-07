import tasker
import logging


class Task(tasker.task.Task):
    name = 'test_task'

    compression = 'none'
    timeout = 30.0
    max_tasks_per_run = 10000
    max_retries = 3
    log_level = logging.INFO

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

    task_queue = tasker.queue.Queue(
        connector=connector,
        queue_name='test_task',
        compression='none',
    )

    # task = Task(
    #     task_queue=task_queue,
    # )

    worker = tasker.worker.Worker(Task, task_queue, 4, False)
    worker.start()

if __name__ == '__main__':
    try:
        main()
    except:
        print('killed')
