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

        self.tasks_consumed = 0

    def dequeue(self, timeout):
        '''
        '''
        self.tasks_consumed += 1

        if timeout == 0:
            timeout = None

        return self.shared_task_queue.get(
            block=True,
            timeout=timeout,
        )

    def dequeue_bulk(self, count):
        '''
        '''
        values = self.task_queue.connector.pop_bulk(
            key=self.task_queue.queue_name,
            count=count,
        )

        decoded_values = []
        for value in values:
            decoded_value = self.task_queue._decode(
                value=value,
            )
            decoded_values.append(decoded_value)

        return decoded_values

    def enqueue(self, value):
        '''
        '''
        self.task_queue.enqueue(
            value=value,
        )

    def enqueue_bulk(self, values):
        '''
        '''
        encoded_values = []
        for value in values:
            encoded_value = self.task_queue._encode(
                value=value,
            )
            encoded_values.append(encoded_value)

        pushed = self.task_queue.connector.push_bulk(
            key=self.task_queue.queue_name,
            values=encoded_values,
        )

        return pushed

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

    def __init__(self, task_class, task_queue, monitor_client, concurrent_workers, autoscale):
        self.logger = self._create_logger()

        self.concurrent_workers = concurrent_workers
        self.autoscale = autoscale

        multiprocessing_manager = multiprocessing.Manager()
        multiprocessing_queue = multiprocessing_manager.Queue(
            maxsize=task_class.tasks_per_transaction * concurrent_workers,
        )
        self.workers_shared_queue = WorkersSharedQueue(
            task_queue=task_queue,
            shared_task_queue=multiprocessing_queue,
        )
        self.task = task_class(
            task_queue=self.workers_shared_queue,
            monitor_client=monitor_client,
        )

        self.workers_watchdogs_thread_pool = multiprocessing.pool.ThreadPool(
            processes=self.concurrent_workers + 1,
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
            shared_queue_size = self.workers_shared_queue.shared_task_queue.qsize()
            task_queue_size = self.workers_shared_queue.len()

            if shared_queue_size > (max_queue_size / 2) or task_queue_size == 0:
                time.sleep(0.2)
            
                continue

            values = self.workers_shared_queue.dequeue_bulk(
                count=max_queue_size,
            )

            if not values:
                time.sleep(0.2)
            else:
                for value in values:
                    self.workers_shared_queue.shared_task_queue.put(value)

    def worker_watchdog(self, function, stop_event):
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
                        'stop_event': stop_event,
                    }
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

        multiprocessing_manager = multiprocessing.Manager()
        stop_event = multiprocessing_manager.Event()
        stop_event.set()

        async_result = self.workers_watchdogs_thread_pool.apply_async(
            func=self.worker_watchdog,
            kwds={
                'function': self.task.work_loop,
                'stop_event': stop_event,
            },
        )
        self.running_workers.append(
            {
                'async_result': async_result,
                'stop_event': stop_event,
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

        shared_task_queue_async_result = self.workers_watchdogs_thread_pool.apply_async(
            func=self.queue_manager,
        )

        async_results = [
            worker['async_result']
            for worker in self.running_workers
        ]
        async_results.append(shared_task_queue_async_result)
        for async_result in async_results:
            async_result.wait()

        self.workers_watchdogs_thread_pool.terminate()

        self.logger.debug('finished')
