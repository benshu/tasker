import datetime
import os
import signal
import logging
import traceback
import threading
import socket
import random
import time

from . import connector
from . import devices
from . import encoder
from . import logger
from . import monitor
from . import queue


class TaskException(Exception):
    pass


class TaskMaxRetriesException(TaskException):
    pass


class TaskRetryException(TaskException):
    pass


class Task:
    '''
    '''
    name = 'task_name'

    compressor = 'zlib'
    serializer = 'msgpack'
    monitoring = {
        'host_name': socket.gethostname(),
        'stats_server': {
            'host': '',
            'port': 9999,
        }
    }
    connector = {
        'type': 'redis',
        'params': {
            'host': 'localhost',
            'port': 6379,
            'database': 0,
        },
    }
    timeout = 30.0
    global_timeout = 0.0
    max_tasks_per_run = 10
    max_retries = 3
    tasks_per_transaction = 10
    report_completion = False
    heartbeat_interval = 10.0

    def __init__(self, abstract=False):
        self.logger = logger.logger.Logger(
            logger_name=self.name,
        )

        if abstract is True:
            return

        queue_connector_obj = connector.__connectors__[self.connector['type']]
        queue_connector = queue_connector_obj(**self.connector['params'])
        self.task_queue = queue.regular.Queue(
            queue_name=self.name,
            connector=queue_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name=self.compressor,
                serializer_name=self.serializer,
            ),
        )

        self.heartbeater = None
        self.monitor_client = None
        if self.monitoring:
            self.monitor_client = monitor.client.StatisticsClient(
                stats_server=self.monitoring['stats_server'],
                host_name=self.monitoring['host_name'],
                worker_name=self.name,
            )

        self.run_forever = False
        if self.max_tasks_per_run == 0:
            self.run_forever = True

        self.current_task = None
        self.tasks_left = 0
        signal.signal(signal.SIGTERM, self.timeout_signal_handler)

        self.logger.debug('initialized')

    def push_task(self, task):
        '''
        '''
        try:
            self.task_queue.enqueue(
                value=task,
            )

            return True
        except Exception as exception:
            self.logger.error(
                msg='could not push task: {exception}'.format(
                    exception=exception,
                )
            )

            return False

    def push_tasks(self, tasks):
        '''
        '''
        try:
            self.task_queue.enqueue_bulk(
                values=tasks,
            )

            return True
        except Exception as exception:
            self.logger.error(
                msg='could not push tasks: {exception}'.format(
                    exception=exception,
                )
            )

            return False

    def pull_task(self):
        '''
        '''
        try:
            task = self.task_queue.dequeue(
                timeout=0,
            )

            return task
        except Exception as exception:
            self.logger.error(
                msg='could not pull task: {exception}'.format(
                    exception=exception,
                )
            )

            return {}

    def pull_tasks(self, count):
        '''
        '''
        try:
            tasks = self.task_queue.dequeue_bulk(
                count=count,
            )

            return tasks
        except Exception as exception:
            self.logger.error(
                msg='could not pull tasks: {exception}'.format(
                    exception=exception,
                )
            )

            return []

    def craft_task(self, *args, **kwargs):
        '''
        '''
        if self.report_completion:
            completion_key = self.create_completion_key()
        else:
            completion_key = None

        task = {
            'date': datetime.datetime.utcnow().timestamp(),
            'args': args,
            'kwargs': kwargs,
            'run_count': 0,
            'completion_key': completion_key,
        }

        return task

    def create_completion_key(self):
        '''
        '''
        added = False

        while not added:
            completion_key = random.randint(0, 9999999999999)
            added = self.task_queue.add_result(
                value=completion_key,
            )

        return completion_key

    def report_complete(self, task):
        '''
        '''
        completion_key = task['completion_key']

        if completion_key:
            removed = self.task_queue.remove_result(
                value=completion_key,
            )

            return removed
        else:
            return True

    def wait_task_finished(self, task):
        '''
        '''
        completion_key = task['completion_key']

        if completion_key:
            has_result = self.task_queue.has_result(
                value=completion_key,
            )
            while has_result:
                has_result = self.task_queue.has_result(
                    value=completion_key,
                )

                time.sleep(0.5)

    def apply_async_one(self, *args, **kwargs):
        '''
        '''
        task = self.craft_task(*args, **kwargs)

        self.push_task(
            task=task,
        )

        self.logger.debug('enqueued a task')

        return task

    def apply_async_many(self, tasks):
        '''
        '''
        self.push_tasks(
            tasks=tasks,
        )

        self.logger.debug('enqueued tasks')

    def get_next_tasks(self, tasks_left):
        '''
        '''
        if self.tasks_per_transaction == 1:
            while True:
                task = self.pull_task()

                if task:
                    return [task]

        if tasks_left > self.tasks_per_transaction:
            tasks = self.pull_tasks(
                count=self.tasks_per_transaction,
            )
        else:
            tasks = self.pull_tasks(
                count=tasks_left,
            )

        if tasks:
            return tasks

        while True:
            task = self.pull_task()

            if task:
                return [task]

    def timeout_signal_handler(self, signal, frame):
        if self.tasks_left > 0:
            raise TimeoutError()

    def kill_task(self):
        '''
        '''
        os.kill(os.getpid(), signal.SIGTERM)

    def work_loop(self):
        '''
        '''
        try:
            tasks_to_finish = []

            if self.monitoring:
                self.heartbeater = devices.heartbeater.Heartbeater(
                    monitor_client=self.monitor_client,
                    interval=self.heartbeat_interval,
                )
                self.heartbeater.start()

            self.init()

            self.tasks_left = self.max_tasks_per_run
            while self.tasks_left > 0 or self.run_forever is True:
                tasks = self.get_next_tasks(
                    tasks_left=self.tasks_left,
                )

                self.logger.debug(
                    'dequeued {tasks_dequeued} tasks'.format(
                        tasks_dequeued=len(tasks),
                    )
                )

                tasks_to_finish = tasks.copy()
                for task in tasks:
                    self.current_task = task
                    task_execution_result = self.execute_task(
                        task=task,
                    )
                    tasks_to_finish.remove(task)

                    if task_execution_result == 'timeout':
                        return

                    if task_execution_result != 'retry':
                        self.report_complete(
                            task=task,
                        )

                    if not self.run_forever:
                        self.tasks_left -= 1

                self.logger.debug('task execution finished')
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )
            self.logger.error(
                msg=traceback.format_exc(),
            )
            self.logger.error(
                msg=self.current_task,
            )

            raise exception
        finally:
            if tasks_to_finish:
                try:
                    self.push_tasks(tasks_to_finish)
                except:
                    pass

            logging.shutdown()

            if self.heartbeater:
                self.heartbeater.stop()

    def execute_task(self, task):
        '''
        '''
        try:
            self.killer_timer = None
            if self.timeout > 0:
                self.killer_timer = threading.Timer(
                    self.timeout,
                    self.kill_task,
                )
                self.killer_timer.start()

            returned_value = self.work(
                *task['args'],
                **task['kwargs']
            )
            if self.killer_timer:
                self.killer_timer.cancel()
                self.killer_timer = None

            self.logger.debug('task succeeded')

            self._on_success(
                returned_value=returned_value,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'success'
        except TimeoutError as exception:
            self.logger.debug('task execution timed out')

            self._on_timeout(
                exception=exception,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'timeout'
        except TaskRetryException as exception:
            self.logger.debug('task retry has called')

            self._on_retry(
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'retry'
        except TaskMaxRetriesException as exception:
            self.logger.debug('max retries exceeded')

            self._on_max_retries(
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'max_retries'
        except Exception as exception:
            self.logger.debug('task execution failed')
            self._on_failure(
                exception=exception,
                args=task['args'],
                kwargs=task['kwargs'],
            )

            return 'exception'
        finally:
            if self.killer_timer:
                self.killer_timer.cancel()
                self.killer_timer = None

    def retry(self):
        '''
        '''
        if self.max_retries <= self.current_task['run_count']:
            raise TaskMaxRetriesException(
                'max retries of: {max_retries}, exceeded'.format(
                    max_retries=self.max_retries,
                )
            )

        task = self.current_task
        task['run_count'] += 1

        self.push_task(
            task=task,
        )

        self.logger.debug('task retry enqueued')

        raise TaskRetryException

    def _on_success(self, returned_value, args, kwargs):
        '''
        '''
        if self.monitoring:
            self.monitor_client.send_success()

        self.logger.info(
            'task {task_name} reported a success:\n\tvalue: {value}\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                value=returned_value,
                args=args,
                kwargs=kwargs,
            )
        )

        try:
            self.on_success(
                returned_value=returned_value,
                args=args,
                kwargs=kwargs,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

    def _on_failure(self, exception, args, kwargs):
        '''
        '''
        if self.monitoring:
            self.monitor_client.send_failure()

        self.logger.error(
            'task {task_name} reported a failure:\n\texception: {exception}\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                exception=exception,
                args=args,
                kwargs=kwargs,
            )
        )

        try:
            self.on_failure(
                exception=exception,
                args=args,
                kwargs=kwargs,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

    def _on_retry(self, args, kwargs):
        '''
        '''
        if self.monitoring:
            self.monitor_client.send_retry()

        self.logger.warning(
            'task {task_name} asked for a retry:\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                args=args,
                kwargs=kwargs,
            )
        )

        try:
            self.on_retry(
                args=args,
                kwargs=kwargs,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

    def _on_timeout(self, exception, args, kwargs):
        '''
        '''
        if self.monitoring:
            self.monitor_client.send_failure()

        self.logger.error(
            'task {task_name} raised a timeout exception: {exception}\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                exception=str(exception),
                args=args,
                kwargs=kwargs,
            )
        )

        try:
            self.on_timeout(
                exception=exception,
                args=args,
                kwargs=kwargs,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

    def _on_max_retries(self, args, kwargs):
        '''
        '''
        if self.monitoring:
            self.monitor_client.send_failure()

        self.logger.error(
            'task {task_name} has reached the max retries:\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                args=args,
                kwargs=kwargs,
            )
        )

        try:
            self.on_max_retries(
                args=args,
                kwargs=kwargs,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
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

    def on_failure(self, exception, args, kwargs):
        '''
        '''
        pass

    def on_retry(self, args, kwargs):
        '''
        '''
        pass

    def on_max_retries(self, args, kwargs):
        '''
        '''
        pass

    def on_timeout(self, exception, args, kwargs):
        '''
        '''
        pass

    def __getstate__(self):
        '''
        '''
        state = {
            'name': self.name,
            'compressor': self.compressor,
            'serializer': self.serializer,
            'monitoring': self.monitoring,
            'connector': self.connector,
            'timeout': self.timeout,
            'max_tasks_per_run': self.max_tasks_per_run,
            'max_retries': self.max_retries,
            'tasks_per_transaction': self.tasks_per_transaction,
            'report_completion': self.report_completion,
            'heartbeat_interval': self.heartbeat_interval,
        }

        return state

    def __setstate__(self, value):
        '''
        '''
        self.name = value['name']
        self.compressor = value['compressor']
        self.serializer = value['serializer']
        self.monitoring = value['monitoring']
        self.connector = value['connector']
        self.timeout = value['timeout']
        self.max_tasks_per_run = value['max_tasks_per_run']
        self.max_retries = value['max_retries']
        self.tasks_per_transaction = value['tasks_per_transaction']
        self.report_completion = value['report_completion']
        self.heartbeat_interval = value['heartbeat_interval']

        self.__init__(
            abstract=False,
        )
