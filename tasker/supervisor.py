import sys
import os
import threading
import multiprocessing
import multiprocessing.pool
import traceback

from . import logger


class SafeProcess(
    multiprocessing.Process,
):
    def __init__(
        self,
        *args,
        **kwargs
    ):
        super().__init__(
            *args,
            **kwargs
        )

        pipe = multiprocessing.Pipe()

        self._parent_pipe = pipe[0]
        self._child_pipe = pipe[1]
        self._exception = None

    def run(
        self,
    ):
        try:
            multiprocessing.Process.run(
                self,
            )
            self._child_pipe.send(None)
        except Exception as exception:
            self._child_pipe.send(
                {
                    'exception': exception,
                    'traceback': traceback.format_exc(),
                }
            )

    @property
    def exception(
        self,
    ):
        if self._parent_pipe.poll():
            self._exception = self._parent_pipe.recv()

        return self._exception


class Supervisor:
    def __init__(
        self,
        worker_class,
        concurrent_workers,
    ):
        self.logger = logger.logger.Logger(
            logger_name='Supervisor',
        )

        self.worker_class = worker_class
        self.concurrent_workers = concurrent_workers

        self.task = self.worker_class()

        self.workers_processes = []

        self.should_work_event = threading.Event()
        self.should_work_event.set()

        multiprocessing.set_start_method(
            method='spawn',
            force=True,
        )

    def worker_watchdog(
        self,
        function,
    ):
        process = None

        while self.should_work_event.is_set():
            try:
                process = SafeProcess(
                    target=function,
                    kwargs={},
                )
                process.start()

                self.workers_processes.append(process)

                if self.task.config['timeouts']['global_timeout'] != 0.0:
                    process.join(
                        timeout=self.task.global_timeout,
                    )
                else:
                    process.join(
                        timeout=None,
                    )

                if process.exception:
                    self.logger.critical(
                        msg='supervisor has thrown an exception',
                        extra={
                            'exception': {
                                'type': str(type(process.exception['exception'])),
                                'message': str(process.exception['exception']),
                            },
                            'traceback': process.exception['traceback'],
                            'additional': dict(),
                        },
                    )
            except Exception as exception:
                self.logger.critical(
                    msg='supervisor has thrown an exception',
                    extra={
                        'exception': {
                            'type': str(type(exception)),
                            'message': str(exception),
                        },
                        'traceback': traceback.format_exc(),
                        'additional': dict(),
                    },
                )
            finally:
                if process:
                    process.terminate()

                    try:
                        os.waitpid(
                            process.pid,
                            0,
                        )
                    except ChildProcessError:
                        pass

                    self.workers_processes.remove(process)

    def start(
        self,
    ):
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

    def __getstate__(
        self,
    ):
        state = {
            'worker_class': self.worker_class,
            'concurrent_workers': self.concurrent_workers,
        }

        return state

    def __setstate__(
        self,
        value,
    ):
        self.__init__(
            worker_class=value['worker_class'],
            concurrent_workers=value['concurrent_workers'],
        )
