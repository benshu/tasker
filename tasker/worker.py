import os
import signal
import traceback
import socket

from . import connector
from . import devices
from . import encoder
from . import logger
from . import monitor
from . import queue
from . import task_queue


class Worker:
    '''
    '''
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
                'database': 0,
            },
        },
        'timeouts': {
            'soft_timeout': 30.0,
            'hard_timeout': 35.0,
            'global_timeout': 0.0,
        },
        'max_tasks_per_run': 10,
        'max_retries': 3,
        'tasks_per_transaction': 10,
        'report_completion': False,
        'heartbeat_interval': 10.0,
    }

    def __init__(self, abstract=False):
        self.logger = logger.logger.Logger(
            logger_name=self.name,
        )

        if abstract is True:
            return

        self.run_forever = False
        if self.config['max_tasks_per_run'] == 0:
            self.run_forever = True

        queue_connector_obj = connector.__connectors__[self.config['connector']['type']]
        queue_connector = queue_connector_obj(**self.config['connector']['params'])
        queue_obj = queue.regular.Queue(
            connector=queue_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name=self.config['encoder']['compressor'],
                serializer_name=self.config['encoder']['serializer'],
            ),
        )
        self.task_queue = task_queue.TaskQueue(
            queue=queue_obj,
        )

    def purge_tasks(self):
        '''
        '''
        return self.task_queue.purge_tasks(
            task_name=self.name,
        )

    def number_of_enqueued_tasks(self):
        '''
        '''
        return self.task_queue.number_of_enqueued_tasks(
            task_name=self.name,
        )

    def craft_task(self, *args, **kwargs):
        '''
        '''
        task = self.task_queue.craft_task(
            task_name=self.name,
            args=args,
            kwargs=kwargs,
            report_completion=self.config['report_completion'],
        )

        return task

    def report_complete(self, task):
        '''
        '''
        return self.task_queue.report_complete(
            task=task,
        )

    def wait_task_finished(self, task, timeout=0):
        '''
        '''
        self.task_queue.wait_task_finished(
            task=task,
            timeout=timeout,
        )

    def apply_async_one(self, *args, **kwargs):
        '''
        '''
        task = self.craft_task(*args, **kwargs)

        self.task_queue.apply_async_one(
            task=task,
        )

        return task

    def apply_async_many(self, tasks):
        '''
        '''
        return self.task_queue.apply_async_many(
            tasks=tasks,
        )

    def get_next_tasks(self, tasks_left):
        '''
        '''
        if tasks_left > self.config['tasks_per_transaction']:
            return self.task_queue.get_tasks(
                task_name=self.name,
                num_of_tasks=self.config['tasks_per_transaction'],
            )
        else:
            return self.task_queue.get_tasks(
                task_name=self.name,
                num_of_tasks=tasks_left,
            )

    def sigabrt_handler(self, signal_num, frame):
        '''
        '''
        try:
            self.tasks_to_finish.remove(self.current_task)

            self._on_timeout(
                exception=TimeoutError('hard timeout'),
                args=self.current_task['args'],
                kwargs=self.current_task['kwargs'],
            )
        except:
            pass

        self.end_working()

        os.kill(os.getpid(), signal.SIGTERM)

    def sigint_handler(self, signal_num, frame):
        '''
        '''
        raise TimeoutError()

    def begin_working(self):
        '''
        '''
        self.tasks_to_finish = []
        self.current_task = None
        self.tasks_left = 0

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

        self.killer = devices.killer.LocalKiller(
            soft_timeout=self.config['timeouts']['soft_timeout'],
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=self.config['timeouts']['hard_timeout'],
            hard_timeout_signal=signal.SIGABRT,
        )
        signal.signal(signal.SIGABRT, self.sigabrt_handler)
        signal.signal(signal.SIGINT, self.sigint_handler)

        self.init()

    def end_working(self):
        '''
        '''
        if self.tasks_to_finish:
            self.task_queue.apply_async_many(
                tasks=self.tasks_to_finish,
            )

        signal.signal(signal.SIGABRT, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        self.heartbeater.stop()
        self.killer.stop()

    def work_loop(self):
        '''
        '''
        try:
            self.begin_working()

            self.tasks_left = self.config['max_tasks_per_run']
            while self.tasks_left > 0 or self.run_forever is True:
                tasks = self.get_next_tasks(
                    tasks_left=self.tasks_left,
                )

                self.monitor_client.increment_process(
                    value=len(tasks),
                )

                self.tasks_to_finish = tasks.copy()
                for task in tasks:
                    self.current_task = task
                    task_execution_result = self.execute_task(
                        task=task,
                    )
                    self.tasks_to_finish.remove(task)

                    if task_execution_result != 'retry':
                        self.report_complete(
                            task=task,
                        )

                    if not self.run_forever:
                        self.tasks_left -= 1
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.logger.log_task_failure(
                failure_reason='Task Execution Exception',
                task_name=self.name,
                args=self.current_task['args'],
                kwargs=self.current_task['kwargs'],
                exception=exception,
                exception_traceback=exception_traceback,
            )
        finally:
            self.end_working()

    def execute_task(self, task):
        '''
        '''
        try:
            self.killer.reset()
            self.killer.start()

            returned_value = self.work(
                *task['args'],
                **task['kwargs']
            )

            self.killer.stop()

            self._on_success(
                returned_value=returned_value,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'success'
        except TimeoutError as exception:
            exception_traceback = traceback.format_exc()
            self._on_timeout(
                exception=exception,
                exception_traceback=exception_traceback,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'timeout'
        except WorkerRetryException as exception:
            self._on_retry(
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'retry'
        except WorkerMaxRetriesException as exception:
            exception_traceback = traceback.format_exc()
            self._on_max_retries(
                exception=exception,
                exception_traceback=exception_traceback,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'max_retries'
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self._on_failure(
                exception=exception,
                exception_traceback=exception_traceback,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'exception'
        finally:
            self.killer.stop()

    def retry(self):
        '''
        '''
        if self.config['max_retries'] <= self.current_task['run_count']:
            raise WorkerMaxRetriesException(
                'max retries of: {max_retries}, exceeded'.format(
                    max_retries=self.config['max_retries'],
                )
            )

        self.task_queue.retry(
            task=self.current_task,
        )

        raise WorkerRetryException

    def _on_success(self, returned_value, args, kwargs):
        '''
        '''
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

    def _on_failure(self, exception, exception_traceback, args, kwargs):
        '''
        '''
        self.monitor_client.increment_failure()

        self.logger.log_task_failure(
            failure_reason='Failue',
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

    def _on_retry(self, args, kwargs):
        '''
        '''
        self.monitor_client.increment_retry()

        self.logger.log_task_retry(
            task_name=self.name,
            args=args,
            kwargs=kwargs,
        )

        try:
            self.on_retry(
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

    def _on_timeout(self, exception, exception_traceback, args, kwargs):
        '''
        '''
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

    def _on_max_retries(self, exception, exception_traceback, args, kwargs):
        '''
        '''
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

    def init(self):
        '''
        '''
        pass

    def work(self):
        '''
        '''
        pass

    def on_success(self, returned_value, args, kwargs):
        '''
        '''
        pass

    def on_failure(self, exception, exception_traceback, args, kwargs):
        '''
        '''
        pass

    def on_retry(self, args, kwargs):
        '''
        '''
        pass

    def on_max_retries(self, exception, exception_traceback, args, kwargs):
        '''
        '''
        pass

    def on_timeout(self, exception, exception_traceback, args, kwargs):
        '''
        '''
        pass

    def __getstate__(self):
        '''
        '''
        state = {
            'name': self.name,
            'config': self.config,
        }

        return state

    def __setstate__(self, value):
        '''
        '''
        self.name = value['name']
        self.config = value['config']

        self.__init__(
            abstract=False,
        )


class WorkerException(Exception):
    pass


class WorkerMaxRetriesException(WorkerException):
    pass


class WorkerRetryException(WorkerException):
    pass
