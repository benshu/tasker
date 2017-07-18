import traceback

from .. import worker
from .. import profiler


class Executor:
    def __init__(
        self,
        work_method,
        update_current_task,
        on_success,
        on_timeout,
        on_failure,
        worker_profiling_handler,
        worker_config,
        worker_name,
        worker_logger,
        worker_task_queue,
    ):
        self.work_method = work_method
        self.update_current_task = update_current_task
        self.on_success = on_success
        self.on_timeout = on_timeout
        self.on_failure = on_failure
        self.worker_profiling_handler = worker_profiling_handler
        self.worker_config = worker_config
        self.worker_name = worker_name
        self.worker_logger = worker_logger
        self.worker_task_queue = worker_task_queue
        self.tasks_to_finish = []

    def begin_working(
        self,
    ):
        pass

    def end_working(
        self,
    ):
        pass

    def pre_work(
        self,
        task,
    ):
        pass

    def post_work(
        self,
    ):
        pass

    def execute_task_encapsulated(
        self,
        task,
    ):
        returned_value = None
        raised_exception = None
        success_execution = True

        self.pre_work(
            task=task,
        )

        if self.worker_config['profiler']['enabled']:
            work_profiler = profiler.profiler.Profiler()
            work_profiler.start()

        try:
            returned_value = self.work_method(
                *task['args'],
                **task['kwargs'],
            )
        except Exception as exception:
            success_execution = False
            raised_exception = exception

        if self.worker_config['profiler']['enabled']:
            work_profiler.stop()

            self.worker_profiling_handler(
                profiling_data=work_profiler.profiling_results(
                    num_of_slowest_methods=self.worker_config['profiler']['num_of_slowest_methods_to_log'],
                ),
                args=task['args'],
                kwargs=task['kwargs'],
            )

        self.post_work()

        return {
            'success': success_execution,
            'returned_value': returned_value,
            'exception': raised_exception,
        }

    def execute_task(
        self,
        task,
    ):
        execution_result = self.execute_task_encapsulated(
            task=task,
        )

        if execution_result['success']:
            self.on_success(
                task=task,
                returned_value=execution_result['returned_value'],
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return

        exception = execution_result['exception']
        exception_traceback = ''.join(
            traceback.format_tb(
                tb=exception.__traceback__,
            ),
        )

        if isinstance(
            execution_result['exception'],
            (
                worker.WorkerSoftTimedout,
                worker.WorkerHardTimedout,
            )
        ):
            self.on_timeout(
                task=task,
                exception=exception,
                exception_traceback=exception_traceback,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return
        else:
            self.on_failure(
                task=task,
                exception=exception,
                exception_traceback=exception_traceback,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return
