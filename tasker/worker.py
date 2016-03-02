import logging
import time
import threading
import multiprocessing
import multiprocessing.pool


class Worker:
    '''
    '''
    def __init__(self, task, concurrent_workers, autoscale):
        self.logger = self._create_logger()

        self.task = task
        self.concurrent_workers = concurrent_workers
        self.autoscale = autoscale

        self.pool_context = multiprocessing.get_context('spawn')
        self.pool = multiprocessing.pool.Pool(
            processes=concurrent_workers,
            context=self.pool_context,
        )
        self.workers_semaphore = threading.Semaphore(
            value=concurrent_workers,
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
        logger.setLevel(logging.DEBUG)

        return logger

    def worker_watchdog(self, function):
        '''
        '''
        self.logger.debug('started')

        while self.workers_semaphore.acquire():
            self.logger.debug('task applied')

            async_result = self.pool.apply_async(
                func=function,
            )

            async_result.wait()

            self.logger.debug('task finished')

            try:
                async_result.get()
            except Exception as exc:
                self.logger.error(
                    'task execution raised an exception: {exc}'.format(
                        exc=exc,
                    )
                )

            self.workers_semaphore.release()

    def add_worker(self):
        '''
        '''
        self.logger.debug('ADDING')
        if self.workers_semaphore._value < self.concurrent_workers:
            self.workers_semaphore.release()

            self.logger.debug('one worker added')

    def remove_worker(self):
        '''
        '''
        self.logger.debug('REMOVING')
        if self.workers_semaphore._value > 0:
            self.workers_semaphore.acquire()

            self.logger.debug('one worker removed')

    def autoscaler(self):
        '''
        '''
        empty_queue_counter = 0

        while True:
            pending_tasks = self.task.queue.len()

            if pending_tasks == 0:
                empty_queue_counter += 1
            else:
                empty_queue_counter = 0
                self.add_worker()

            if empty_queue_counter > 5:
                self.remove_worker()

            time.sleep(1)

    def start(self):
        '''
        '''
        watchdog_threads = []

        self.logger.debug('started')

        if self.autoscale:
            autoscaler_thread = threading.Thread(
                target=self.autoscaler,
            )
            autoscaler_thread.start()

        for i in range(self.concurrent_workers):
            watchdog_thread = threading.Thread(
                target=self.worker_watchdog,
                kwargs={
                    'function': self.task.work_loop,
                }
            )
            watchdog_thread.start()

            watchdog_threads.append(watchdog_thread)

        map(lambda thread: thread.join(), watchdog_threads)
        autoscaler_thread.join()

        self.logger.debug('finished')
