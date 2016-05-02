import logging
import multiprocessing
import multiprocessing.pool

from . import logger


class Worker:
    '''
    '''
    log_level = logging.WARNING

    def __init__(self, task_class, concurrent_workers):
        self.logger = logger.logger.Logger(
            logger_name='Worker',
            log_level=self.log_level,
        )

        self.task_class = task_class
        self.concurrent_workers = concurrent_workers

        self.task = self.task_class()

        self.workers_watchdogs_thread_pool = multiprocessing.pool.ThreadPool(
            processes=self.concurrent_workers,
        )

        self.logger.debug('initialized')

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
                async_result.get()

                self.logger.debug('task finished')
            except Exception as exception:
                self.logger.error(
                    'task execution raised an exception: {exception}'.format(
                        exception=exception,
                    )
                )
            finally:
                process_pool.terminate()

    def start(self):
        '''
        '''
        self.logger.debug('started')

        async_results = []
        for i in range(self.concurrent_workers):
            async_result = self.workers_watchdogs_thread_pool.apply_async(
                func=self.worker_watchdog,
                kwds={
                    'function': self.task.work_loop,
                },
            )
            async_results.append(async_result)

        for async_result in async_results:
            async_result.wait()

        self.workers_watchdogs_thread_pool.terminate()

        self.logger.debug('finished')

    def __getstate__(self):
        '''
        '''
        state = {
            'task_class': self.task_class,
            'concurrent_workers': self.concurrent_workers,
        }

        self.logger.debug('getstate')

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            task_class=value['task_class'],
            concurrent_workers=value['concurrent_workers'],
        )

        self.logger.debug('setstate')
