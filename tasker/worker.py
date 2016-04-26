import logging
import multiprocessing
import multiprocessing.pool
import zmq

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

    def __init__(self, task_class, connector_obj, concurrent_workers, autoscale):
        self.logger = self._create_logger()

        self.concurrent_workers = concurrent_workers
        self.autoscale = autoscale

        self.task_queue = queue.regular.Queue(
            queue_name=task_class.name,
            connector=connector_obj,
        )

        self.tasks_streamer = TasksStreamer()

        zmq_connector = connector.zmq.Connector(
            push_address='tcp://127.0.0.1:{push_port}'.format(
                push_port=self.tasks_streamer.frontend_port,
            ),
            pull_address='tcp://127.0.0.1:{pull_port}'.format(
                pull_port=self.tasks_streamer.backend_port,
            ),
        )
        self.workers_shared_queue = queue.regular.Queue(
            queue_name=task_class.name,
            connector=zmq_connector,
        )
        self.task = task_class(
            task_queue=self.workers_shared_queue,
        )

        self.workers_watchdogs_thread_pool = multiprocessing.pool.ThreadPool(
            processes=self.concurrent_workers + 2,
        )

        self.running_workers = []

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

    def queue_manager(self):
        '''
        '''
        max_queue_size = self.task.tasks_per_transaction * self.concurrent_workers

        while True:
            values = self.task_queue.dequeue_bulk(
                count=max_queue_size,
            )

            self.workers_shared_queue.enqueue_bulk(values)

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

    def add_worker(self):
        '''
        '''
        if len(self.running_workers) == self.concurrent_workers:
            self.logger.debug('max concurrent workers are already running')

            return

        async_result = self.workers_watchdogs_thread_pool.apply_async(
            func=self.worker_watchdog,
            kwds={
                'function': self.task.work_loop,
            },
        )
        self.running_workers.append(
            {
                'async_result': async_result,
            }
        )

        self.logger.debug('added another running worker')

    def remove_worker(self):
        '''
        '''
        if len(self.running_workers) == 0:
            self.logger.debug('no running workers left to remove')

            return

        worker_to_remove = self.running_workers.pop()
        worker_to_remove['stop_event'].clear()

        self.logger.debug('a worker has been removed from the queue')

    def start(self):
        '''
        '''
        self.logger.debug('started')

        for i in range(self.concurrent_workers):
            self.add_worker()

        tasks_streamer_async_result = self.workers_watchdogs_thread_pool.apply_async(
            func=self.tasks_streamer.start,
        )
        shared_task_queue_async_result = self.workers_watchdogs_thread_pool.apply_async(
            func=self.queue_manager,
        )

        async_results = [
            worker['async_result']
            for worker in self.running_workers
        ]
        async_results.append(shared_task_queue_async_result)
        async_results.append(tasks_streamer_async_result)
        for async_result in async_results:
            async_result.wait()

        self.workers_watchdogs_thread_pool.terminate()

        self.logger.debug('finished')
