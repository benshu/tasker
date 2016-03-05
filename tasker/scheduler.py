import functools
import datetime
import time
import threading
import asyncio
import logging


class Scheduler:
    '''
    '''
    log_level = logging.INFO

    def __init__(self):
        '''
        '''
        self.logger = self._create_logger()

        self.should_run = threading.Event()
        self.should_run.clear()
        self.should_terminate = threading.Event()
        self.should_run.clear()

        self.work_thread = threading.Thread(
            target=self._loop_main,
        )
        self.work_thread.start()

        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)

        self.logger.debug('initialized')

        self.schedulers_threads = []

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
        logger.setLevel(self.log_level)

        return logger

    def _loop_main(self):
        '''
        '''
        self.logger.debug('started')

        while self.should_run.wait():
            if self.should_terminate.isSet():
                break

            self.logger.debug('run_forever')

            self.event_loop.run_forever()

        self.logger.debug('finished')

    def clear(self):
        '''
        '''
        self.stop()
        self.event_loop.close()

        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)

    def start(self):
        '''
        '''
        self.should_run.set()
        while not self.event_loop.is_running():
            time.sleep(0)

        self.logger.debug('started')

    def stop(self):
        '''
        '''
        self.should_run.clear()
        if self.event_loop.is_running():
            self.event_loop.call_soon_threadsafe(
                callback=self.event_loop.stop,
            )

        while self.event_loop.is_running():
            time.sleep(0)

        self.logger.debug('stopped')

    def terminate(self):
        '''
        '''
        self.logger.debug('terminating')

        self.stop()
        self.clear()

        self.should_terminate.set()
        self.should_run.set()
        self.work_thread.join()
        self.should_run.clear()

        self.logger.debug('terminated')

    def _enqueue_task(self, task, args, kwargs, time_delta, repeatedly):
        '''
        '''
        task.run(
            *args,
            **kwargs,
        )

        self.logger.debug('task enqueued')

        if repeatedly:
            self._run_in(
                task=task,
                args=args,
                kwargs=kwargs,
                time_delta=time_delta,
                repeatedly=repeatedly,
            )

            self.logger.debug('repeated task enqueued')

    def _run_in(self, task, args, kwargs, time_delta, repeatedly):
        '''
        '''
        self.logger.debug('started')

        self.event_loop.call_soon_threadsafe(
            callback=functools.partial(
                self.event_loop.call_later,
                **{
                    'delay': time_delta.total_seconds(),
                    'callback': functools.partial(
                        self._enqueue_task,
                        **{
                            'task': task,
                            'args': args,
                            'kwargs': kwargs,
                            'time_delta': time_delta,
                            'repeatedly': repeatedly,
                        }
                    )
                }
            )
        )

    def run_now(self, task, args, kwargs):
        '''
        '''
        self._run_in(
            task=task,
            args=args,
            kwargs=kwargs,
            time_delta=datetime.timedelta(
                seconds=0,
            ),
            repeatedly=False,
        )

        self.logger.debug('scheduled')

    def run_at(self, task, args, kwargs, date_to_run_at):
        '''
        '''
        time_delta = date_to_run_at - datetime.datetime.utcnow()

        self._run_in(
            task=task,
            args=args,
            kwargs=kwargs,
            time_delta=time_delta,
            repeatedly=False,
        )

        self.logger.debug('scheduled')

    def run_in(self, task, args, kwargs, time_delta):
        '''
        '''
        self._run_in(
            task=task,
            args=args,
            kwargs=kwargs,
            time_delta=time_delta,
            repeatedly=False,
        )

        self.logger.debug('scheduled')

    def run_every(self, task, args, kwargs, time_delta):
        '''
        '''
        self._run_in(
            task=task,
            args=args,
            kwargs=kwargs,
            time_delta=time_delta,
            repeatedly=True,
        )

        self.logger.debug('scheduled')
