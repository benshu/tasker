import tasker
import logging
import time


class Task(tasker.task.Task):
    name = 'test_task'

    compression = 'none'
    timeout = 30.0
    max_tasks_per_run = 10000
    tasks_per_transaction = 1000
    max_retries = 3
    log_level = logging.ERROR

    def init(self):
        self.a = 0

    def work(self, num):
        self.a += num


def main():
    connector = tasker.connectors.redis.Connector(
        host='localhost',
        port=6379,
        database=0,
    )

    task_queue = tasker.queue.Queue(
        connector=connector,
        queue_name='test_task',
        compressor='none',
        serializer='msgpack',
    )
    monitor_client = tasker.monitor.client.StatisticsClient(
        stats_server={
            'host': '127.0.0.1',
            'port': 9999,
        },
        host_name='test_host',
        worker_name='test_worker',
    )

    worker = tasker.worker.Worker(Task, task_queue, monitor_client, 4, False)
    worker.log_level = logging.ERROR
    worker.start()

if __name__ == '__main__':
    try:
        main()
    except:
        print('killed')
