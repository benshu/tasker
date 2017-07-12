import traceback
import os
import signal

from .import _executor
from .. import devices
from .. import worker


class SerialExecutor(
    _executor.Executor,
):
    def sigabrt_handler(
        self,
        signal_num,
        frame,
    ):
        try:
            self.tasks_to_finish.remove(self.current_task)

            self.on_timeout(
                task=self.current_task,
                exception=worker.WorkerHardTimedout(),
                exception_traceback=''.join(traceback.format_stack()),
                args=self.current_task['args'],
                kwargs=self.current_task['kwargs'],
            )
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.worker_logger.log_task_failure(
                failure_reason='Exception during sigabrt_handler',
                task_name=self.worker_name,
                args=self.current_task['args'],
                kwargs=self.current_task['kwargs'],
                exception=exception,
                exception_traceback=exception_traceback,
            )
        finally:
            self.end_working()

            os.kill(os.getpid(), signal.SIGTERM)

    def sigint_handler(
        self,
        signal_num,
        frame,
    ):
        raise worker.WorkerSoftTimedout()

    def begin_working(
        self,
    ):
        if self.worker_config['timeouts']['critical_timeout'] == 0:
            killer_device = devices.killer.LocalKiller
        else:
            killer_device = devices.killer.RemoteKiller
        
        self.killer = killer_device(
            pid=os.getpid(),
            soft_timeout=self.worker_config['timeouts']['soft_timeout'],
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=self.worker_config['timeouts']['hard_timeout'],
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=self.worker_config['timeouts']['critical_timeout'],
            critical_timeout_signal=signal.SIGTERM,
            memory_limit=self.worker_config['limits']['memory'],
            memory_limit_signal=signal.SIGABRT,
        )

        signal.signal(signal.SIGABRT, self.sigabrt_handler)
        signal.signal(signal.SIGINT, self.sigint_handler)

    def end_working(
        self,
    ):
        if self.tasks_to_finish:
            self.worker_task_queue.apply_async_many(
                tasks=self.tasks_to_finish,
            )

        signal.signal(signal.SIGABRT, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        self.killer.stop()

    def execute_tasks(
        self,
        tasks,
    ):
        self.tasks_to_finish = tasks.copy()

        for task in tasks:
            self.current_task = task
            self.execute_task(
                task=task,
            )
            self.tasks_to_finish.remove(task)

    def pre_work(
        self,
        task,
    ):
        self.update_current_task(
            task=task,
        )
        self.killer.reset()
        self.killer.start()

    def post_work(
        self,
    ):
        self.killer.reset()
        self.killer.stop()
