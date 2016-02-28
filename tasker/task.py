import multiprocessing
import multiprocessing.pool
import datetime
import logging

from . import queue


class Task:
    '''
    '''
    name = 'task_name'

    compression = 'none'
    timeout = 30.0
    max_tasks_per_run = 10
    max_retries = 3

    def __init__(self, connector):
        self.logger = self._create_logger()

        self.connector = connector

        self.queue = queue.Queue(
            connector=self.connector,
            queue_name=self.name,
            compression=self.compression,
        )

        self.pool = multiprocessing.pool.ThreadPool(
            processes=1,
            initializer=self.init,
            initargs=(),
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
        logger.setLevel(logging.INFO)

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
        num_of_finished_tasks = 0

        while num_of_finished_tasks <= self.max_tasks_per_run:
            task = self.queue.dequeue(
                timeout=1,
            )

            if task is None:
                continue

            self.logger.debug('dequeued a task')

            self.execute_task(
                task=task,
            )

            self.logger.debug('task execution finished')

            num_of_finished_tasks += 1

        return True

    def execute_task(self, task):
        '''
        '''
        self.last_task = task

        args = task['args']
        kwargs = task['kwargs']

        try:
            async_result = self.pool.apply_async(
                func=self.work,
                args=args,
                kwds=kwargs,
            )

            self.logger.debug('task applied')

            returned_value = async_result.get(
                timeout=self.timeout,
            )

            self.logger.debug('task succeeded')

            self.on_success(
                returned_value=returned_value,
                args=args,
                kwargs=kwargs,
            )
        except multiprocessing.TimeoutError as exc:
            self.logger.debug('task execution timed out')

            self.on_timeout(
                exception=exc,
                args=args,
                kwargs=kwargs,
            )
        except Exception as exc:
            self.logger.debug('task execution failed')

            self.on_failure(
                exception=exc,
                args=args,
                kwargs=kwargs,
            )

    def retry(self):
        '''
        '''
        if self.max_retries <= self.last_task['run_count']:
            raise Exception('max retries exceeded')

        self.on_retry(
            args=self.last_task['args'],
            kwargs=self.last_task['kwargs'],
        )

        task = {
            'insertion_date': datetime.datetime.utcnow(),
            'args': self.last_task['args'],
            'kwargs': self.last_task['kwargs'],
            'run_count': self.last_task['run_count'] + 1,
        }

        self.queue.enqueue(
            task=task,
        )

        self.logger.debug('task retry enqueued')

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
        self.logger.info(
            'task {task_name} reported a success:\n\tvalue: {value}\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                value=returned_value,
                args=args,
                kwargs=kwargs,
            )
        )

    def on_failure(self, exception, args, kwargs):
        '''
        '''
        self.logger.info(
            'task {task_name} reported a failure:\n\texception: {exception}\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                exception=exception,
                args=args,
                kwargs=kwargs,
            )
        )

    def on_retry(self, args, kwargs):
        '''
        '''
        self.logger.info(
            'task {task_name} asked for a retry:\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                args=args,
                kwargs=kwargs,
            )
        )

    def on_timeout(self, exception, args, kwargs):
        '''
        '''
        self.logger.info(
            'task {task_name} raised a timeout exception: {exception}\n\targs: {args}\n\tkwargs: {kwargs}'.format(
                task_name=self.name,
                exception=str(exception),
                args=args,
                kwargs=kwargs,
            )
        )

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
