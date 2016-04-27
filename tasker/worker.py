import logging
import multiprocessing
import multiprocessing.pool
import zmq
import time

from . import queue
from . import connector


class TasksStreamer:
    def __init__(self):
        self.context = zmq.Context()

        self.frontend = self.context.socket(zmq.PUSH)
        self.frontend_port = self.frontend.bind_to_random_port(
            addr='tcp://127.0.0.1',
        )

        self.backend = self.context.socket(zmq.PULL)
        self.backend_port = self.backend.bind_to_random_port(
            addr='tcp://127.0.0.1',
        )

    def start(self):
        try:
            zmq.device(
                device_type=zmq.STREAMER,
                frontend=self.frontend,
                backend=self.backend,
            )
        except Exception as exception:
            print(exception)
        finally:
            self.frontend.close()
            self.backend.close()
            self.context.term()


class Worker:
    '''
    '''
    log_level = logging.WARNING

    def __init__(self, task_class, concurrent_workers, autoscale):
        self.logger = self._create_logger()

        self.task_class = task_class
        self.concurrent_workers = concurrent_workers
        self.autoscale = autoscale

        self.tasks_streamer = TasksStreamer()

        queue_connector = self.get_connector(
            connector_type=self.task_class.connector['type'],
            connector_params=self.task_class.connector['params'],
        )
        self.task_queue = self.get_queue(
            queue_type=self.task_class.queue['type'],
            queue_name=self.task_class.queue['name'],
            queue_connector=queue_connector,
        )

        self.task_class.connector = {
            'type': 'zmq',
            'params': {
                'push_address': 'tcp://127.0.0.1:{push_port}'.format(
                    push_port=self.tasks_streamer.frontend_port,
                ),
                'pull_address': 'tcp://127.0.0.1:{pull_port}'.format(
                    pull_port=self.tasks_streamer.backend_port,
                ),
            },
        }
        self.task = self.task_class()
        self.task.queue_connector.pull_socket.close()

        self.workers_watchdogs_thread_pool = multiprocessing.pool.ThreadPool(
            processes=self.concurrent_workers + 2,
        )

        self.logger.debug('initialized')

    def _create_logger(self):
        '''
        '''
        logger = logging.getLogger(
            name='Worker',
        )

        for handler in logger.handlers:
            logger.removeHandler(handler)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt='%(asctime)s %(name)-12s %(levelname)-8s %(funcName)-16s -> %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.setLevel(self.log_level)

        return logger

    def get_connector(self, connector_type, connector_params):
        '''
        '''
        for connector_obj in connector.__connectors__:
            if connector_obj.name == connector_type:
                return connector_obj(**connector_params)
        else:
            raise Exception(
                'could not find connector: {connector_type}'.format(
                    connector_type=connector_type,
                )
            )

    def get_queue(self, queue_type, queue_name, queue_connector):
        '''
        '''
        for queue_obj in queue.__queues__:
            if queue_obj.name == queue_type:
                return queue_obj(
                    queue_name=queue_name,
                    connector=queue_connector,
                )
        else:
            raise Exception(
                'could not find queue: {queue_type}'.format(
                    queue_type=queue_type,
                )
            )

    def queue_manager(self):
        '''
        '''
        max_queue_size = self.task.tasks_per_transaction * self.concurrent_workers

        while True:
            values = self.task_queue.dequeue_bulk(
                count=max_queue_size,
            )

            self.task.queue_connector.push_bulk(
                key=self.task_class.name,
                values=values,
            )

    def worker_watchdog(self, function):
        '''
        '''
        self.logger.debug('started')

        while True:
            try:
                process_pool_context = multiprocessing.get_context('spawn')
                process_pool = multiprocessing.pool.Pool(
                    processes=1,
                    context=process_pool_context,
                )

                self.logger.debug('task applied')

                async_result = process_pool.apply_async(
                    func=function,
                )
                async_result.wait()

                self.logger.debug('task finished')

                task_execution_result = async_result.get()

                if task_execution_result is False:
                    self.logger.debug('task has been stopped')

                    return False
            except Exception as exception:
                self.logger.error(
                    'task execution raised an exception: {exception}'.format(
                        exception=exception,
                    )
                )
            finally:
                process_pool.terminate()

    def start_task(self):
        '''
        '''
        task = self.task_class()
        task.work_loop()

    def start(self):
        '''
        '''
        self.logger.debug('started')

        async_results = []

        tasks_streamer_async_result = self.workers_watchdogs_thread_pool.apply_async(
            func=self.tasks_streamer.start,
        )
        async_results.append(tasks_streamer_async_result)
        time.sleep(1)

        for i in range(self.concurrent_workers):
            async_result = self.workers_watchdogs_thread_pool.apply_async(
                func=self.worker_watchdog,
                kwds={
                    'function': self.task.work_loop,
                },
            )
            async_results.append(async_result)

        time.sleep(1)

        shared_task_queue_async_result = self.workers_watchdogs_thread_pool.apply_async(
            func=self.queue_manager,
        )
        async_results.append(shared_task_queue_async_result)

        for async_result in async_results:
            async_result.wait()

        self.workers_watchdogs_thread_pool.terminate()

        self.logger.debug('finished')
