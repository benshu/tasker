import queue
import datetime
import time
import threading


class Scheduler:
    '''
    '''
    sleeping_interval = 1

    def __init__(self, task):
        '''
        '''
        self.task = task
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
                task = self.queue.get_nowait()
                self.task.run(
                    *task['args'],
                    **task['kwargs'],
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

    def _run_at(self, time_to_run, args, kwargs):
        '''
        '''
        while datetime.datetime.utcnow() < time_to_run:
            time.sleep(self.sleeping_interval)

        self.queue.put(
            {
                'args': args,
                'kwargs': kwargs,
            }
        )

    def run_at(self, time_to_run_at, args, kwargs):
        '''
        '''
        self.run_at_thread = threading.Thread(
            target=self._run_at,
            kwargs={
                'time_to_run': time_to_run_at,
                'args': args,
                'kwargs': kwargs,
            },
        )
        self.run_at_thread.start()

    def run_within(self, time_delta, args, kwargs):
        '''
        '''
        now = datetime.datetime.utcnow()
        time_to_run_at = now + time_delta

        self.run_at_thread = threading.Thread(
            target=self._run_at,
            kwargs={
                'time_to_run': time_to_run_at,
                'args': args,
                'kwargs': kwargs,
            },
        )
        self.run_at_thread.start()
