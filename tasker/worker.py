import os
import signal
import time
import traceback
import socket
import concurrent.futures

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
            # 'type': 'threaded',
            # 'concurrency': 50,
        },
        'max_tasks_per_run': 10,
        'max_retries': 3,
        'tasks_per_transaction': 10,
        'report_completion': False,
        'heartbeat_interval': 10.0,
    }

    def __init__(self):
        self.logger = logger.logger.Logger(
            logger_name=self.name,
        )

        self.worker_initialized = False

    def init_worker(self):
        if self.worker_initialized:
            return

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

    def work_loop(self):
        '''
        '''
        try:
            self.init_worker()

            if self.config['executor']['type'] == 'serial':
                self.executor = SerialExecutor(
                    worker=self,
                )
            elif self.config['executor']['type'] == 'threaded':
                self.executor = ThreadedExecutor(
                    worker=self,
                )

            self.executor.begin_working()

            run_forever = self.config['max_tasks_per_run'] == 0

            tasks_left = self.config['max_tasks_per_run']
            while tasks_left > 0 or run_forever is True:
                tasks = self.get_next_tasks(
                    tasks_left=tasks_left,
                )
                if not tasks:
                    time.sleep(1)
                    continue

                self.monitor_client.increment_process(
                    value=len(tasks),
                )

                self.executor.execute_tasks(
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
        finally:
            self.executor.end_working()

    def retry(self):
        '''
        '''
        raise WorkerRetry

    def _on_success(self, task, returned_value, args, kwargs):
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

    def _on_failure(self, task, exception, exception_traceback, args, kwargs):
        '''
        '''
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

    def _on_retry(self, task, exception, exception_traceback, args, kwargs):
        '''
        '''
        self.monitor_client.increment_retry()

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

    def _on_timeout(self, task, exception, exception_traceback, args, kwargs):
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

    def _on_max_retries(self, task, exception, exception_traceback, args, kwargs):
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

    def on_retry(self, exception, exception_traceback, args, kwargs):
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

        self.__init__()


class SerialExecutor:
    '''
    '''
    def __init__(self, worker):
        self.worker = worker

    def sigabrt_handler(self, signal_num, frame):
        '''
        '''
        try:
            self.tasks_to_finish.remove(self.current_task)

            self.worker._on_timeout(
                task=self.current_task,
                exception=WorkerHardTimedout(),
                exception_traceback=''.join(traceback.format_stack()),
                args=self.current_task['args'],
                kwargs=self.current_task['kwargs'],
            )
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.logger.log_task_failure(
                failure_reason='Exception during sigabrt_handler',
                task_name=self.name,
                args=self.current_task['args'],
                kwargs=self.current_task['kwargs'],
                exception=exception,
                exception_traceback=exception_traceback,
            )

        self.end_working()

        os.kill(os.getpid(), signal.SIGTERM)

    def sigint_handler(self, signal_num, frame):
        '''
        '''
        raise WorkerSoftTimedout()

    def begin_working(self):
        '''
        '''
        if self.worker.config['timeouts']['critical_timeout'] == 0:
            self.killer = devices.killer.LocalKiller(
                pid=os.getpid(),
                soft_timeout=self.worker.config['timeouts']['soft_timeout'],
                soft_timeout_signal=signal.SIGINT,
                hard_timeout=self.worker.config['timeouts']['hard_timeout'],
                hard_timeout_signal=signal.SIGABRT,
                critical_timeout=self.worker.config['timeouts']['critical_timeout'],
                critical_timeout_signal=signal.SIGTERM,
                memory_limit=self.worker.config['limits']['memory'],
                memory_limit_signal=signal.SIGABRT,
            )
        else:
            self.killer = devices.killer.RemoteKiller(
                pid=os.getpid(),
                soft_timeout=self.worker.config['timeouts']['soft_timeout'],
                soft_timeout_signal=signal.SIGINT,
                hard_timeout=self.worker.config['timeouts']['hard_timeout'],
                hard_timeout_signal=signal.SIGABRT,
                critical_timeout=self.worker.config['timeouts']['critical_timeout'],
                critical_timeout_signal=signal.SIGTERM,
                memory_limit=self.worker.config['limits']['memory'],
                memory_limit_signal=signal.SIGABRT,
            )
        signal.signal(signal.SIGABRT, self.sigabrt_handler)
        signal.signal(signal.SIGINT, self.sigint_handler)

        self.worker.init()

    def end_working(self):
        '''
        '''
        if self.tasks_to_finish:
            self.worker.task_queue.apply_async_many(
                tasks=self.tasks_to_finish,
            )

        signal.signal(signal.SIGABRT, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        self.worker.heartbeater.stop()
        self.killer.stop()

    def execute_tasks(self, tasks):
        '''
        '''
        self.tasks_to_finish = tasks.copy()

        for task in tasks:
            self.current_task = task
            status = self.execute_task(
                task=task,
            )
            self.tasks_to_finish.remove(task)
            if status != 'retry':
                self.worker.report_complete(
                    task=task,
                )

    def execute_task(self, task):
        '''
        '''
        try:
            self.killer.reset()
            self.killer.start()

            returned_value = self.worker.work(
                *task['args'],
                **task['kwargs']
            )

            self.killer.stop()

            self.worker._on_success(
                task=task,
                returned_value=returned_value,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'success'
        except (
            WorkerSoftTimedout,
            WorkerHardTimedout,
        ) as exception:
            exception_traceback = traceback.format_exc()
            self.worker._on_timeout(
                task=task,
                exception=exception,
                exception_traceback=exception_traceback,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'timeout'
        except WorkerRetry as exception:
            exception_traceback = traceback.format_exc()

            if self.worker.config['max_retries'] <= task['run_count']:
                self.worker._on_max_retries(
                    task=task,
                    exception=exception,
                    exception_traceback=exception_traceback,
                    args=task['args'],
                    kwargs=task['kwargs'],
                )

                return 'max_retries'
            else:
                self.worker._on_retry(
                    task=task,
                    exception=exception,
                    exception_traceback=exception_traceback,
                    args=task['args'],
                    kwargs=task['kwargs'],
                )

                return 'retry'
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.worker._on_failure(
                task=task,
                exception=exception,
                exception_traceback=exception_traceback,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'failure'
        finally:
            self.killer.reset()
            self.killer.stop()


class ThreadedExecutor:
    '''
    '''
    def __init__(self, worker):
        self.worker = worker
        self.concurrency = worker.config['executor']['concurrency']

    def begin_working(self):
        '''
        '''
        self.worker.init()

    def end_working(self):
        '''
        '''
        self.worker.heartbeater.stop()

    def execute_tasks(self, tasks):
        '''
        '''
        future_to_task = {}

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.concurrency,
        ) as executor:
            for task in tasks:
                future = executor.submit(self.execute_task, task)
                future_to_task[future] = task

        for future in concurrent.futures.as_completed(future_to_task):
            task = future_to_task[future]

            status = future.result()
            if status != 'retry':
                self.worker.report_complete(
                    task=task,
                )

    def execute_task(self, task):
        '''
        '''
        try:
            returned_value = self.worker.work(
                *task['args'],
                **task['kwargs']
            )

            self.worker._on_success(
                task=task,
                returned_value=returned_value,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'success'
        except WorkerRetry as exception:
            exception_traceback = traceback.format_exc()

            if self.worker.config['max_retries'] <= task['run_count']:
                self.worker._on_max_retries(
                    task=task,
                    exception=exception,
                    exception_traceback=exception_traceback,
                    args=task['args'],
                    kwargs=task['kwargs'],
                )

                return 'max_retries'
            else:
                self.worker._on_retry(
                    task=task,
                    exception=exception,
                    exception_traceback=exception_traceback,
                    args=task['args'],
                    kwargs=task['kwargs'],
                )

                return 'retry'
        except Exception as exception:
            exception_traceback = traceback.format_exc()
            self.worker._on_failure(
                task=task,
                exception=exception,
                exception_traceback=exception_traceback,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'failure'


class WorkerException(Exception):
    pass


class WorkerSoftTimedout(WorkerException):
    pass


class WorkerHardTimedout(WorkerException):
    pass


class WorkerRetry(WorkerException):
    pass
