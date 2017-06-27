import tasker
import time
import socket


class Worker(
    tasker.worker.Worker,
):
    name = 'test_worker'
    config = {
        'encoder': {
            'compressor': 'dummy',
            'serializer': 'pickle',
        },
        'monitoring': {
            'host_name': socket.gethostname(),
            'stats_server': {
                'host': 'localhost',
                'port': 9999,
            }
        },
        'connector': {
            'type': 'redis',
            'params': {
                'host': 'localhost',
                'port': 6380,
                'password': 'e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
                'database': 0,
            },
        },
        'timeouts': {
            'soft_timeout': 3.0,
            'hard_timeout': 35.0,
            'critical_timeout': 0.0,
            'global_timeout': 0.0,
        },
        'limits': {
            'memory': 0,
        },
        'executor': {
            'type': 'serial',
            # 'type': 'threaded',
            # 'concurrency': 50,
        },
        'max_tasks_per_run': 25000,
        'tasks_per_transaction': 5000,
        'max_retries': 3,
        'report_completion': False,
        'heartbeat_interval': 10.0,
    }

    def init(
        self,
    ):
        pass

    def work(
        self,
        run_type,
    ):
        if run_type == 'start':
            self.logger.error('start')
            self.logger.error(time.time())
        elif run_type == 'end':
            self.logger.error('end')
            self.logger.error(time.time())


def main():
    worker = Worker()
    worker.init_worker()

    worker.apply_async_one(
        run_type='start',
    )

    tasks = []
    for j in range(99998):
        task_obj = worker.craft_task(
            run_type='',
        )
        tasks.append(task_obj)
    worker.apply_async_many(
        tasks=tasks,
    )

    worker.apply_async_one(
        run_type='end',
    )

    supervisor = tasker.supervisor.Supervisor(
        worker_class=Worker,
        concurrent_workers=4,
    )
    supervisor.start()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
        print('killed')
