import multiprocessing
import multiprocessing.pool

from . import logger


class Worker:
    '''
    '''

    def __init__(self, task_class, concurrent_workers):
        self.logger = logger.logger.Logger(
            logger_name='Worker',
        )

        self.task_class = task_class
        self.concurrent_workers = concurrent_workers

        self.task = self.task_class(
            abstract=True,
        )

        self.logger.debug('initialized')

    def worker_watchdog(self, function):
        '''
        '''
        self.logger.debug('started')

        while True:
            try:
                context = multiprocessing.get_context(
                    method='spawn',
                )
                worker_process_pool = multiprocessing.pool.Pool(
                    processes=1,
                    context=context,
                )
                async_result = worker_process_pool.apply_async(
                    func=function,
                )
                async_result.wait(
                    timeout=None,
                )
                async_result.get(
                    timeout=5,
                )
            except Exception as exception:
                print(exception)
                self.logger.error(
                    'task execution raised an exception: {exception}'.format(
                        exception=exception,
                    )
                )
            finally:
                worker_process_pool.terminate()

    def start(self):
        '''
        '''
        self.logger.debug('started')

        worker_managers_thread_pool = multiprocessing.pool.ThreadPool(
            processes=self.concurrent_workers,
        )

        async_results = []
        for i in range(self.concurrent_workers):
            async_result = worker_managers_thread_pool.apply_async(
                func=self.worker_watchdog,
                kwds={
                    'function': self.task.work_loop,
                },
            )
            async_results.append(async_result)

        for async_result in async_results:
            async_result.wait()

        worker_managers_thread_pool.terminate()

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
