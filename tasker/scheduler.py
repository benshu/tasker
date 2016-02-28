import queue
import datetime
import time
import threading


class Scheduler:
    '''
    '''
    sleeping_interval = 1.0

    def __init__(self):
        '''
        '''
        self.queue = queue.Queue()

        self.run_lock = threading.Lock()
        self.run_lock.acquire()

        self.work_thread = threading.Thread(
            target=self.work_loop,
        )
        self.work_thread.start()

    def work_loop(self):
        '''
        '''
        while self.run_lock.acquire():
            try:
                queued_task = self.queue.get_nowait()

                queued_task['task'].run(
                    *queued_task['args'],
                    **queued_task['kwargs']
                )
            except queue.Empty:
                continue
            finally:
                self.run_lock.release()

    def start(self):
        '''
        '''
        self.run_lock.release()

    def stop(self):
        '''
        '''
        self.run_lock.acquire()

    def _run_at(self, task, date_to_run_at, args, kwargs):
        '''
        '''
        while datetime.datetime.utcnow() < date_to_run_at:
            time.sleep(self.sleeping_interval)

        self.queue.put(
            {
                'task': task,
                'args': args,
                'kwargs': kwargs,
            }
        )

    def run_at(self, task, date_to_run_at, args, kwargs):
        '''
        '''
        self.run_at_thread = threading.Thread(
            target=self._run_at,
            kwargs={
                'task': task,
                'date_to_run_at': date_to_run_at,
                'args': args,
                'kwargs': kwargs,
            },
        )
        self.run_at_thread.start()

    def run_within(self, task, time_delta, args, kwargs):
        '''
        '''
        now = datetime.datetime.utcnow()
        date_to_run_at = now + time_delta

        self.run_at_thread = threading.Thread(
            target=self._run_at,
            kwargs={
                'task': task,
                'date_to_run_at': date_to_run_at,
                'args': args,
                'kwargs': kwargs,
            },
        )
        self.run_at_thread.start()

    def run_every(self, task, time_delta, args, kwargs):
        '''
        '''
        now = datetime.datetime.utcnow()
        date_to_run_at = now + time_delta

        self.run_at_thread = threading.Thread(
            target=self._run_at,
            kwargs={
                'task': task,
                'date_to_run_at': date_to_run_at,
                'args': args,
                'kwargs': kwargs,
            },
        )
        self.run_at_thread.start()
