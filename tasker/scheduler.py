import queue
import datetime
import threading
import logging


class Scheduler:
    '''
    '''
    def __init__(self):
        '''
        '''
        self.logger = self._create_logger()

        self.queue = queue.Queue()

        self.run_lock = threading.Lock()
        self.run_lock.acquire()

        self.work_thread = threading.Thread(
            target=self.schedule_loop,
        )
        self.work_thread.start()

        self.logger.debug('initialized')

    def _create_logger(self):
        '''
        '''
        logger = logging.getLogger(
            name='Scheduler',
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

    def schedule_loop(self):
        '''
        '''
        self.logger.debug('started')

        while self.run_lock.acquire():
            try:
                queued_task = self.queue.get_nowait()

                self.logger.debug('new task received')

                queued_task['task'].run(
                    *queued_task['args'],
                    **queued_task['kwargs']
                )

                self.logger.debug('new task enqueued')
            except queue.Empty:
                continue
            finally:
                self.run_lock.release()

        self.logger.debug('finished')

    def start(self):
        '''
        '''
        self.run_lock.release()

        self.logger.debug('started')

    def stop(self):
        '''
        '''
        self.run_lock.acquire()

        self.logger.debug('stopped')

    def _enqueue_task(self, task, args, kwargs):
        '''
        '''
        self.queue.put(
            {
                'task': task,
                'args': args,
                'kwargs': kwargs,
            }
        )

        self.logger.debug('task enqueued to the self.queue')

    def _run_in(self, time_delta, task, args, kwargs, repeatedly):
        '''
        '''
        self.logger.debug('started')

        timer = threading.Timer(
            interval=time_delta.total_seconds(),
            function=self._enqueue_task,
            kwargs={
                'task': task,
                'args': args,
                'kwargs': kwargs,
            },
        )
        timer.start()

        self.logger.debug('timer started')

        if repeatedly:
            timer = threading.Timer(
                interval=time_delta.total_seconds(),
                function=self._run_in,
                kwargs={
                    'time_delta': time_delta,
                    'task': task,
                    'args': args,
                    'kwargs': kwargs,
                    'repeatedly': repeatedly,
                },
            )
            timer.start()

            self.logger.debug('repeated timer started')

    def run_now(self, task, args, kwargs):
        '''
        '''
        self._enqueue_task(
            task=task,
            args=args,
            kwargs=kwargs,
        )

        self.logger.debug('scheduled')

    def run_at(self, date_to_run_at, task, args, kwargs):
        '''
        '''
        time_delta = date_to_run_at - datetime.datetime.utcnow()

        self._run_in(
            time_delta=time_delta,
            task=task,
            args=args,
            kwargs=kwargs,
            repeatedly=False,
        )

        self.logger.debug('scheduled')

    def run_in(self, time_delta, task, args, kwargs):
        '''
        '''
        self._run_in(
            time_delta=time_delta,
            task=task,
            args=args,
            kwargs=kwargs,
            repeatedly=False,
        )

        self.logger.debug('scheduled')

    def run_every(self, time_delta, task, args, kwargs):
        '''
        '''
        self._run_in(
            time_delta=time_delta,
            task=task,
            args=args,
            kwargs=kwargs,
            repeatedly=True,
        )

        self.logger.debug('scheduled')
