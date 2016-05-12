import functools
import datetime
import time
import threading
import asyncio

from . import logger


class Scheduler:
    '''
    '''
    def __init__(self):
        '''
        '''
        self.logger = logger.logger.Logger(
            logger_name='Scheduler',
        )

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

        self.schedulers_threads = []

    def _loop_main(self):
        '''
        '''
        while self.should_run.wait():
            if self.should_terminate.isSet():
                break

            self.event_loop.run_forever()

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

    def terminate(self):
        '''
        '''
        self.stop()
        self.clear()

        self.should_terminate.set()
        self.should_run.set()
        self.work_thread.join()
        self.should_run.clear()

    def _enqueue_task(self, task, args, kwargs, time_delta, repeatedly):
        '''
        '''
        task.apply_async_one(
            *args,
            **kwargs
        )

        if repeatedly:
            self._run_in(
                task=task,
                args=args,
                kwargs=kwargs,
                time_delta=time_delta,
                repeatedly=repeatedly,
            )

    def _run_in(self, task, args, kwargs, time_delta, repeatedly):
        '''
        '''
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
