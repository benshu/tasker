import logging
import multiprocessing
import multiprocessing.pool
import time


class WorkersSharedQueue:
    def __init__(self, task_queue, shared_task_queue):
        super().__init__()

        self.task_queue = task_queue
        self.shared_task_queue = shared_task_queue

    def dequeue(self, timeout):
        return self.shared_task_queue.get()

    def enqueue(self, value):
        self.task_queue.enqueue(
            value=value,
        )


class Worker:
    '''
    '''
    log_level = logging.INFO

    def __init__(self, task, task_queue, concurrent_workers, autoscale):
        self.logger = self._create_logger()

        self.task = task
        self.task_queue = task_queue
        self.concurrent_workers = concurrent_workers
        self.autoscale = autoscale

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

    def queue_manager(self, shared_queue):
        '''
        '''
        while True:
            time.sleep(0.1)
            values = shared_queue.task_queue.dequeue_bulk(
                count=1000,
            )

            if not values:
                continue
            else:
                for value in values:
                    shared_queue.shared_task_queue.put(value)

    def worker_watchdog(self, function, shared_queue):
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
                    kwds={
                        'task_queue': shared_queue,
                    }
                )

                async_result.wait()

                self.logger.debug('task finished')

                async_result.get()
            except Exception as exc:
                self.logger.error(
                    'task execution raised an exception: {exc}'.format(
                        exc=exc,
                    )
                )
            finally:
                process_pool.terminate()

    def start(self):
        '''
        '''
        async_results = []
        manager = multiprocessing.Manager()
        shared_task_queue = manager.Queue()
        shared_queue = WorkersSharedQueue(
            task_queue=self.task_queue,
            shared_task_queue=shared_task_queue,
        )

        self.logger.debug('started')

        thread_pool = multiprocessing.pool.ThreadPool(
            processes=self.concurrent_workers + 1,
        )

        for i in range(self.concurrent_workers):
            async_result = thread_pool.apply_async(
                func=self.worker_watchdog,
                kwds={
                    'function': self.task.work_loop,
                    'shared_queue': shared_queue,
                }
            )
            async_results.append(async_result)

        async_result = thread_pool.apply_async(
            func=self.queue_manager,
            kwds={
                'shared_queue': shared_queue,
            }
        )
        async_results.append(async_result)

        for async_result in async_results:
            async_result.wait()

        thread_pool.terminate()

        self.logger.debug('finished')
