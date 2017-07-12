import time
import traceback
import socket
import threading

from . import connector
from . import devices
from . import encoder
from . import logger
from . import monitor
from . import queue
from . import storage
from . import task_queue
from . import executor


class Worker:
    name = 'worker_name'
    config = {
        'encoder': {
            'compressor': 'dummy',
            'serializer': 'pickle',
        },
        'monitoring': {
            'host_name': socket.gethostname(),
            'stats_server': {
                'host': '',
                'port': 9999,
            }
        },
        'connector': {
            'type': 'redis',
            'params': {
                'host': 'localhost',
                'port': 6379,
                'password': 'e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
                'database': 0,
            },
        },
        'timeouts': {
            'soft_timeout': 30.0,
            'hard_timeout': 35.0,
            'critical_timeout': 0.0,
            'global_timeout': 0.0,
        },
        'limits': {
            'memory': 0,
        },
        'executor': {
            'type': 'serial',
        },
        'profiler': {
            'enabled': False,
            'num_of_slowest_methods_to_log': 20,
        },
        'max_tasks_per_run': 10,
        'max_retries': 3,
        'tasks_per_transaction': 10,
        'report_completion': False,
        'heartbeat_interval': 10.0,
    }

    def __init__(
        self,
    ):
        self.logger = logger.logger.Logger(
            logger_name=self.name,
        )

        self.worker_initialized = False
        self.current_tasks = {}

    def init_worker(
        self,
    ):
        if self.worker_initialized:
            return

        encoder_obj = encoder.encoder.Encoder(
            compressor_name=self.config['encoder']['compressor'],
            serializer_name=self.config['encoder']['serializer'],
        )
        connector_class = connector.__connectors__[self.config['connector']['type']]
        connector_obj = connector_class(**self.config['connector']['params'])
        queue_obj = queue.regular.Queue(
            connector=connector_obj,
            encoder=encoder_obj,
        )

        self.task_queue = task_queue.TaskQueue(
            queue=queue_obj,
        )
        self.storage = storage.storage.Storage(
            connector=connector_obj,
            encoder=encoder_obj,
        )

        if self.config['monitoring']:
            self.monitor_client = monitor.client.StatisticsClient(
                stats_server=self.config['monitoring']['stats_server'],
                host_name=self.config['monitoring']['host_name'],
                worker_name=self.name,
            )
            self.heartbeater = devices.heartbeater.Heartbeater(
                monitor_client=self.monitor_client,
                interval=self.config['heartbeat_interval'],
            )
            self.heartbeater.start()
        else:
            self.monitor_client = monitor.client.StatisticsDummyClient(
                stats_server=self.config['monitoring']['stats_server'],
                host_name=self.config['monitoring']['host_name'],
                worker_name=self.name,
            )
            self.heartbeater = devices.heartbeater.DummyHeartbeater()

        self.worker_initialized = True

    @property
    def current_task(
        self,
    ):
        return self.current_tasks[threading.get_ident()]

    @current_task.setter
    def current_task(
        self,
        value,
    ):
        self.current_tasks[threading.get_ident()] = value

    def purge_tasks(
        self,
    ):
        return self.task_queue.purge_tasks(
            task_name=self.name,
        )

    def number_of_enqueued_tasks(
        self,
    ):
        return self.task_queue.number_of_enqueued_tasks(
            task_name=self.name,
        )

    def craft_task(
        self,
        *args,
        **kwargs
    ):
        task = self.task_queue.craft_task(
            task_name=self.name,
            args=args,
            kwargs=kwargs,
            report_completion=self.config['report_completion'],
        )

        return task

    def report_complete(
        self,
        task,
    ):
        return self.task_queue.report_complete(
            task=task,
        )

    def wait_task_finished(
        self,
        task,
        timeout=0,
    ):
        self.task_queue.wait_task_finished(
            task=task,
            timeout=timeout,
        )

    def apply_async_one(
        self,
        *args,
        **kwargs
    ):
        task = self.craft_task(*args, **kwargs)

        self.task_queue.apply_async_one(
            task=task,
        )

        return task

    def apply_async_many(
        self,
        tasks,
    ):
        return self.task_queue.apply_async_many(
            tasks=tasks,
        )

    def get_next_tasks(
        self,
        number_of_tasks,
    ):
        if number_of_tasks > self.config['tasks_per_transaction']:
            return self.task_queue.get_tasks(
                task_name=self.name,
                number_of_tasks=self.config['tasks_per_transaction'],
            )
        else:
            return self.task_queue.get_tasks(
                task_name=self.name,
                number_of_tasks=number_of_tasks,
            )

    def update_current_task(
        self,
        task,
    ):
        self.current_task = task

    def work_loop(
        self,
    ):
        executor_obj = None

        try:
            self.init_worker()

            executor_kwargs = {
                'work_method': self.work,
                'update_current_task': self.update_current_task,
                'on_success': self._on_success,
                'on_timeout': self._on_timeout,
                'on_failure': self._on_failure,
                'worker_profiling_handler': self.profiling_handler,
                'worker_config': self.config,
                'worker_name': self.name,
                'worker_logger': self.logger,
                'worker_task_queue': self.task_queue,
            }

            if self.config['executor']['type'] == 'serial':
                executor_obj = executor.serial.SerialExecutor(
                    **executor_kwargs,
                )
            elif self.config['executor']['type'] == 'threaded':
                executor_obj = executor.threaded.ThreadedExecutor(
                    **executor_kwargs,
                )

            executor_obj.begin_working()
            self.init()

            run_forever = self.config['max_tasks_per_run'] == 0
            tasks_left = self.config['max_tasks_per_run']

            while tasks_left > 0 or run_forever is True:
                if run_forever:
                    number_of_tasks = self.config['tasks_per_transaction']
                else:
                    number_of_tasks = tasks_left

                tasks = self.get_next_tasks(
                    number_of_tasks=number_of_tasks,
                )
                if not tasks:
                    time.sleep(1)

                    continue

                self.monitor_client.increment_process(
                    value=len(tasks),
                )

                executor_obj.execute_tasks(
                    tasks=tasks,
                )

                if not run_forever:
                    tasks_left -= len(tasks)
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.logger.log_task_failure(
                failure_reason='Task Execution Exception',
                task_name=self.name,
                args=(),
                kwargs={},
                exception=exception,
                exception_traceback=exception_traceback,
            )

            raise exception
        finally:
            if self.heartbeater:
                self.heartbeater.stop()

            if executor_obj:
                executor_obj.end_working()

    def retry(
        self,
        exception=None,
    ):
        task = self.current_task
        exception_traceback = ''.join(traceback.format_stack())

        if not exception:
            exception = WorkerRetry()

        if self.config['max_retries'] <= task['run_count']:
            self._on_max_retries(
                task=task,
                exception=exception,
                exception_traceback=exception_traceback,
                args=task['args'],
                kwargs=task['kwargs'],
            )
        else:
            self._on_retry(
                task=task,
                exception=exception,
                exception_traceback=exception_traceback,
                args=task['args'],
                kwargs=task['kwargs'],
            )

    def requeue(
        self,
        exception=None,
    ):
        task = self.current_task
        exception_traceback = ''.join(traceback.format_stack())

        if not exception:
            exception = WorkerRequeue()

        self._on_requeue(
            task=task,
            exception=exception,
            exception_traceback=exception_traceback,
            args=task['args'],
            kwargs=task['kwargs'],
        )

    def _on_success(
        self,
        task,
        returned_value,
        args,
        kwargs,
    ):
        self.report_complete(
            task=task,
        )
        self.monitor_client.increment_success()

        self.logger.log_task_success(
            task_name=self.name,
            args=args,
            kwargs=kwargs,
            returned_value=returned_value,
        )

        try:
            self.on_success(
                returned_value=returned_value,
                args=args,
                kwargs=kwargs,
            )
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.logger.log_task_failure(
                failure_reason='Exception during on_success',
                task_name=self.name,
                args=args,
                kwargs=kwargs,
                exception=exception,
                exception_traceback=exception_traceback,
            )

    def _on_failure(
        self,
        task,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        self.report_complete(
            task=task,
        )
        self.monitor_client.increment_failure()

        self.logger.log_task_failure(
            failure_reason='Failure',
            task_name=self.name,
            args=args,
            kwargs=kwargs,
            exception=exception,
            exception_traceback=exception_traceback,
        )

        try:
            self.on_failure(
                exception=exception,
                exception_traceback=exception_traceback,
                args=args,
                kwargs=kwargs,
            )
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.logger.log_task_failure(
                failure_reason='Exception during on_failure',
                task_name=self.name,
                args=args,
                kwargs=kwargs,
                exception=exception,
                exception_traceback=exception_traceback,
            )

    def _on_retry(
        self,
        task,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        self.monitor_client.increment_retry(
            value=1,
        )

        self.logger.log_task_failure(
            failure_reason='Retry',
            task_name=self.name,
            args=args,
            kwargs=kwargs,
            exception=exception,
            exception_traceback=exception_traceback,
        )

        try:
            self.on_retry(
                exception=exception,
                exception_traceback=exception_traceback,
                args=args,
                kwargs=kwargs,
            )
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.logger.log_task_failure(
                failure_reason='Exception during on_retry',
                task_name=self.name,
                args=args,
                kwargs=kwargs,
                exception=exception,
                exception_traceback=exception_traceback,
            )

        self.task_queue.retry(
            task=task,
        )

    def _on_requeue(
        self,
        task,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        self.logger.log_task_failure(
            failure_reason='Requeue',
            task_name=self.name,
            args=args,
            kwargs=kwargs,
            exception=exception,
            exception_traceback=exception_traceback,
        )

        try:
            self.on_requeue(
                exception=exception,
                exception_traceback=exception_traceback,
                args=args,
                kwargs=kwargs,
            )
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.logger.log_task_failure(
                failure_reason='Exception during on_requeue',
                task_name=self.name,
                args=args,
                kwargs=kwargs,
                exception=exception,
                exception_traceback=exception_traceback,
            )

        self.task_queue.requeue(
            task=task,
        )

    def _on_timeout(
        self,
        task,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        self.report_complete(
            task=task,
        )
        self.monitor_client.increment_failure()

        self.logger.log_task_failure(
            failure_reason='Timeout',
            task_name=self.name,
            args=args,
            kwargs=kwargs,
            exception=exception,
            exception_traceback=exception_traceback,
        )

        try:
            self.on_timeout(
                exception=exception,
                exception_traceback=exception_traceback,
                args=args,
                kwargs=kwargs,
            )
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.logger.log_task_failure(
                failure_reason='Exception during on_timeout',
                task_name=self.name,
                args=args,
                kwargs=kwargs,
                exception=exception,
                exception_traceback=exception_traceback,
            )

    def _on_max_retries(
        self,
        task,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        self.report_complete(
            task=task,
        )
        self.monitor_client.increment_failure()

        self.logger.log_task_failure(
            failure_reason='Max Retries',
            task_name=self.name,
            args=args,
            kwargs=kwargs,
            exception=exception,
            exception_traceback=exception_traceback,
        )

        try:
            self.on_max_retries(
                exception=exception,
                exception_traceback=exception_traceback,
                args=args,
                kwargs=kwargs,
            )
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.logger.log_task_failure(
                failure_reason='Exception during on_max_retries',
                task_name=self.name,
                args=args,
                kwargs=kwargs,
                exception=exception,
                exception_traceback=exception_traceback,
            )

    def init(
        self,
    ):
        pass

    def work(
        self,
    ):
        pass

    def on_success(
        self,
        returned_value,
        args,
        kwargs,
    ):
        pass

    def on_failure(
        self,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        pass

    def on_retry(
        self,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        pass

    def on_requeue(
        self,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        pass

    def on_max_retries(
        self,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        pass

    def on_timeout(
        self,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        pass

    def profiling_handler(
        self,
        profiling_data,
        args,
        kwargs,
    ):
        pass

    def __getstate__(
        self,
    ):
        state = {
            'name': self.name,
            'config': self.config,
        }

        return state

    def __setstate__(
        self,
        value,
    ):
        self.name = value['name']
        self.config = value['config']

        self.__init__()


class WorkerException(
    Exception,
):
    pass


class WorkerSoftTimedout(
    WorkerException,
):
    pass


class WorkerHardTimedout(
    WorkerException,
):
    pass


class WorkerRequeue(
    WorkerException,
):
    pass


class WorkerRetry(
    WorkerException,
):
    pass
