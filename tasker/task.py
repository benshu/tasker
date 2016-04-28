import multiprocessing
import multiprocessing.pool
import datetime
import logging

from . import connector
from . import encoder
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

    queue = {
        'type': 'regular',
        'name': 'task_name',
    }
    compressor = 'zlib'
    serializer = 'msgpack'
    monitoring = {
        'host_name': '',
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
    max_tasks_per_run = 10
    max_retries = 3
    tasks_per_transaction = 10
    log_level = logging.INFO

    def __init__(self):
        self.logger = self._create_logger()

        queue_connector_obj = connector.__connectors__[self.connector['type']]
        self.queue_connector = queue_connector_obj(**self.connector['params'])

        queue_obj = queue.__queues__[self.queue['type']]
        self.task_queue = queue_obj(
            queue_name=self.queue['name'],
            connector=self.queue_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name=self.compressor,
                serializer_name=self.serializer,
            ),
        )

        self.monitor_client = monitor.client.StatisticsClient(
            stats_server=self.monitoring['stats_server'],
            host_name=self.monitoring['host_name'],
            worker_name=self.name,
        )

        self.logger.debug('initialized')

    def _create_logger(self):
        '''
        '''
        logger = logging.getLogger(
            name='Task',
        )

        for handler in logger.handlers:
            logger.removeHandler(handler)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt='%(asctime)s %(name)-12s %(levelname)-8s %(funcName)-16s -> %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.setLevel(self.log_level)

        return logger

    def push_task(self, task):
        '''
        '''
        self.task_queue.enqueue(
            value=task,
        )

    def pull_task(self):
        '''
        '''
        task = self.task_queue.dequeue(
            timeout=0,
        )

        return task

    def pull_tasks(self, count):
        '''
        '''
        tasks = self.task_queue.dequeue_bulk(
            count=count,
        )

        return tasks

    def apply_async(self, *args, **kwargs):
        '''
        '''
        task = {
            'date': datetime.datetime.utcnow().timestamp(),
            'args': args,
            'kwargs': kwargs,
            'run_count': 0,
        }

        self.push_task(
            task=task,
        )

        self.logger.debug('enqueued a task')

    def work_loop(self):
        '''
        '''
        self.pool = multiprocessing.pool.ThreadPool(
            processes=1,
            initializer=self.init,
            initargs=(),
        )

        tasks_left = self.max_tasks_per_run
        while tasks_left:
            if tasks_left > self.tasks_per_transaction:
                tasks = self.pull_tasks(
                    count=self.tasks_per_transaction,
                )
            else:
                tasks = self.pull_tasks(
                    count=tasks_left,
                )

            if len(tasks) == 0:
                task = self.pull_task()
                tasks = [task]

            self.logger.debug(
                'dequeued {tasks_dequeued} tasks'.format(
                    tasks_dequeued=len(tasks),
                )
            )

            for task in tasks:
                self.execute_task(
                    task=task,
                )
                tasks_left -= 1

            self.logger.debug('task execution finished')

        self.pool.terminate()
        logging.shutdown()

    def execute_task(self, task):
        '''
        '''
        self.last_task = task

        try:
            async_result = self.pool.apply_async(
                func=self.work,
                args=task['args'],
                kwds=task['kwargs'],
            )

            self.logger.debug('task applied')

            returned_value = async_result.get(
                timeout=self.timeout,
            )

            self.logger.debug('task succeeded')

            self._on_success(
                returned_value=returned_value,
                args=task['args'],
                kwargs=task['kwargs'],
            )
        except multiprocessing.TimeoutError as exception:
            self.logger.debug('task execution timed out')

            self._on_timeout(
                exception=exception,
                args=task['args'],
                kwargs=task['kwargs'],
            )
        except TaskRetryException as exception:
            self.logger.debug('task retry has called')

            self._on_retry(
                args=task['args'],
                kwargs=task['kwargs'],
            )
        except TaskMaxRetriesException as exception:
            self.logger.debug('max retries exceeded')

            self._on_max_retries(
                args=task['args'],
                kwargs=task['kwargs'],
            )
        except Exception as exception:
            self.logger.debug('task execution failed')
            print(exception)
            self._on_failure(
                exception=exception,
                args=task['args'],
                kwargs=task['kwargs'],
            )

    def retry(self):
        '''
        '''
        if self.max_retries <= self.last_task['run_count']:
            raise TaskMaxRetriesException(
                'max retries of: {max_retries}, exceeded'.format(
                    max_retries=self.max_retries,
                )
            )

        task = {
            'date': datetime.datetime.utcnow().timestamp(),
            'args': self.last_task['args'],
            'kwargs': self.last_task['kwargs'],
            'run_count': self.last_task['run_count'] + 1,
        }

        self.push_task(
            task=task,
        )

        self.logger.debug('task retry enqueued')

        raise TaskRetryException

    def _on_success(self, returned_value, args, kwargs):
        '''
        '''
        self.monitor_client.send_success()
        self.logger.info(
            'task {task_name} reported a success:\n\tvalue: {value}\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                value=returned_value,
                args=args,
                kwargs=kwargs,
            )
        )

        self.on_success(
            returned_value=returned_value,
            args=args,
            kwargs=kwargs,
        )

    def _on_failure(self, exception, args, kwargs):
        '''
        '''
        self.monitor_client.send_failure()
        self.logger.error(
            'task {task_name} reported a failure:\n\texception: {exception}\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                exception=exception,
                args=args,
                kwargs=kwargs,
            )
        )

        self.on_failure(
            exception=exception,
            args=args,
            kwargs=kwargs,
        )

    def _on_retry(self, args, kwargs):
        '''
        '''
        self.monitor_client.send_retry()
        self.logger.warning(
            'task {task_name} asked for a retry:\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                args=args,
                kwargs=kwargs,
            )
        )

        self.on_retry(
            args=args,
            kwargs=kwargs,
        )

    def _on_timeout(self, exception, args, kwargs):
        '''
        '''
        self.logger.error(
            'task {task_name} raised a timeout exception: {exception}\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                exception=str(exception),
                args=args,
                kwargs=kwargs,
            )
        )

        self.on_timeout(
            exception=exception,
            args=args,
            kwargs=kwargs,
        )

    def _on_max_retries(self, args, kwargs):
        '''
        '''
        self.logger.error(
            'task {task_name} has reached the max retries:\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                args=args,
                kwargs=kwargs,
            )
        )

        self.on_max_retries(
            args=args,
            kwargs=kwargs,
        )

    def init(self):
        '''
        '''
        pass

    def work(self, test_param):
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
            'queue': self.queue,
            'compressor': self.compressor,
            'serializer': self.serializer,
            'monitoring': self.monitoring,
            'connector': self.connector,
            'timeout': self.timeout,
            'max_tasks_per_run': self.max_tasks_per_run,
            'max_retries': self.max_retries,
            'tasks_per_transaction': self.tasks_per_transaction,
            'log_level': self.log_level,
        }

        self.logger.debug('getstate')

        return state

    def __setstate__(self, value):
        '''
        '''
        self.name = value['name']
        self.queue = value['queue']
        self.compressor = value['compressor']
        self.serializer = value['serializer']
        self.monitoring = value['monitoring']
        self.connector = value['connector']
        self.timeout = value['timeout']
        self.max_tasks_per_run = value['max_tasks_per_run']
        self.max_retries = value['max_retries']
        self.tasks_per_transaction = value['tasks_per_transaction']
        self.log_level = value['log_level']

        self.__init__()

        self.logger.debug('setstate')
