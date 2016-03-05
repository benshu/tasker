import logging
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
        logger.setLevel(logging.INFO)

        return logger

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
        watchdog_async_results = []

        self.logger.debug('started')

        thread_pool = multiprocessing.pool.ThreadPool(
            processes=self.concurrent_workers,
        )

        for i in range(self.concurrent_workers):
            async_result = thread_pool.apply_async(
                func=self.worker_watchdog,
                kwds={
                    'function': self.task.work_loop,
                }
            )
            watchdog_async_results.append(async_result)

        for async_result in watchdog_async_results:
            async_result.wait()

        thread_pool.terminate()

        self.logger.debug('finished')
