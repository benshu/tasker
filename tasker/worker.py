import logging
import multiprocessing
import multiprocessing.pool
import time


class WorkersSharedQueue:
    '''
    '''
    def __init__(self, task_queue, shared_task_queue):
        super().__init__()

        self.task_queue = task_queue
        self.shared_task_queue = shared_task_queue

    def dequeue(self, timeout):
        '''
        '''
        return self.shared_task_queue.get()

    def enqueue(self, value):
        '''
        '''
        self.task_queue.enqueue(
            value=value,
        )

    def qsize(self):
        '''
        '''
        return self.shared_task_queue.qsize()

    def len(self):
        '''
        '''
        return self.task_queue.len()

    def flush(self):
        '''
        '''
        return self.task_queue.flush()

    def __getstate__(self):
        '''
        '''
        state = {
            'task_queue': self.task_queue,
            'shared_task_queue': self.shared_task_queue,
        }

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            task_queue=value['task_queue'],
            shared_task_queue=value['shared_task_queue'],
        )


class Worker:
    '''
    '''
    log_level = logging.WARNING

    def __init__(self, task_class, task_queue, concurrent_workers, autoscale):
        self.logger = self._create_logger()

        self.concurrent_workers = concurrent_workers
        self.autoscale = autoscale

        multiprocessing_manager = multiprocessing.Manager()
        multiprocessing_queue = multiprocessing_manager.Queue(
            maxsize=task_class.tasks_per_transaction * concurrent_workers,
        )
        self.shared_task_queue = WorkersSharedQueue(
            task_queue=task_queue,
            shared_task_queue=multiprocessing_queue,
        )
        self.task = task_class(
            task_queue=self.shared_task_queue,
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

    def queue_manager(self):
        '''
        '''
        max_queue_size = self.task.tasks_per_transaction * self.concurrent_workers

        while True:
            shared_queue_size = self.shared_task_queue.qsize()
            task_queue_size = self.shared_task_queue.len()

            if shared_queue_size > (max_queue_size / 2) or task_queue_size == 0:
                time.sleep(0)

                continue

            values = self.shared_task_queue.task_queue.dequeue_bulk(
                count=max_queue_size,
            )

            if not values:
                time.sleep(0)
            else:
                for value in values:
                    self.shared_task_queue.shared_task_queue.put(value)

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

        self.logger.debug('started')

        thread_pool = multiprocessing.pool.ThreadPool(
            processes=self.concurrent_workers + 1,
        )

        for i in range(self.concurrent_workers):
            async_result = thread_pool.apply_async(
                func=self.worker_watchdog,
                kwds={
                    'function': self.task.work_loop,
                }
            )
            async_results.append(async_result)

        async_result = thread_pool.apply_async(
            func=self.queue_manager,
        )
        async_results.append(async_result)

        for async_result in async_results:
            async_result.wait()

        thread_pool.terminate()

        self.logger.debug('finished')
