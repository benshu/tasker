import multiprocessing
import multiprocessing.pool
import datetime
import logging

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

    compression = 'none'
    timeout = 30.0
    max_tasks_per_run = 10
    max_retries = 3
    log_level = logging.INFO

    def __init__(self, connector):
        self.logger = self._create_logger()

        self.connector = connector

        self.queue = queue.Queue(
            connector=self.connector,
            queue_name=self.name,
            compression=self.compression,
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

    def run(self, *args, **kwargs):
        '''
        '''
        task = {
            'insertion_date': datetime.datetime.utcnow(),
            'args': args,
            'kwargs': kwargs,
            'run_count': 0,
        }

        self.queue.enqueue(
            value=task,
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

        for i in range(self.max_tasks_per_run):
            task = self.queue.dequeue(
                timeout=0,
            )

            self.logger.debug('dequeued a task')

            self.execute_task(
                task=task,
            )

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
            'insertion_date': datetime.datetime.utcnow(),
            'args': self.last_task['args'],
            'kwargs': self.last_task['kwargs'],
            'run_count': self.last_task['run_count'] + 1,
        }

        self.queue.enqueue(
            value=task,
        )

        self.logger.debug('task retry enqueued')

        raise TaskRetryException

    def _on_success(self, returned_value, args, kwargs):
        '''
        '''
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
        connector = self.connector

        self.logger.debug('getstate')

        return connector

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            connector=value,
        )

        self.logger.debug('setstate')
