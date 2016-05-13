import sys
import threading
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

        self.workers_processes = []

        self.should_work_event = threading.Event()
        self.should_work_event.set()

    def worker_watchdog(self, function):
        '''
        '''
        while self.should_work_event.is_set():
            try:
                context = multiprocessing.get_context(
                    method='spawn',
                )
                process = context.Process(
                    target=function,
                )
                process.start()
                self.workers_processes.append(process)

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
                process.terminate()
                self.workers_processes.remove(process)

    def start(self):
        '''
        '''
        threads = []
        for i in range(self.concurrent_workers):
            thread = threading.Thread(
                target=self.worker_watchdog,
                kwargs={
                    'function': self.task.work_loop,
                },
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)

        try:
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            pass
        except Exception as exception:
            print(exception)
        finally:
            self.should_work_event.clear()
            for worker_process in self.workers_processes:
                worker_process.terminate()
            sys.exit(0)

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
