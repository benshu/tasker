import os
import signal
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

    def worker_watchdog(self, function):
        '''
        '''
        while True:
            try:
                context = multiprocessing.get_context(
                    method='spawn',
                )
                process = context.Process(
                    target=function,
                )
                process.start()

                if self.task.global_timeout != 0.0:
                    process.join(
                        timeout=self.task.global_timeout,
                    )
                else:
                    process.join(
                        timeout=None,
                    )
            except Exception as exception:
                self.logger.error(
                    'task execution raised an exception: {exception}'.format(
                        exception=exception,
                    )
                )
            finally:
                os.kill(process.pid, signal.SIGTERM)

    def start(self):
        '''
        '''
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

        try:
            for async_result in async_results:
                async_result.wait()
        except:
            pass
        finally:
            worker_managers_thread_pool.terminate()

    def __getstate__(self):
        '''
        '''
        state = {
            'task_class': self.task_class,
            'concurrent_workers': self.concurrent_workers,
        }

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            task_class=value['task_class'],
            concurrent_workers=value['concurrent_workers'],
        )
