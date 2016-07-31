import time
import threading
import multiprocessing
import os
import psutil


class LocalKiller:
    '''
    '''
    def __init__(
        self,
        pid,
        soft_timeout,
        soft_timeout_signal,
        hard_timeout,
        hard_timeout_signal,
        critical_timeout,
        critical_timeout_signal,
    ):
        self.sleep_interval = 0.5

        self.soft_timeout = soft_timeout
        self.hard_timeout = hard_timeout
        self.critical_timeout = critical_timeout

        self.soft_timeout_signal = soft_timeout_signal
        self.hard_timeout_signal = hard_timeout_signal
        self.critical_timeout_signal = critical_timeout_signal

        self.time_elapsed = 0.0

        self.stop_event = threading.Event()
        self.stop_event.clear()

        self.created = False

        self.pid_to_kill = pid

    def killing_loop(self):
        '''
        '''
        while self.stop_event.wait():
            if not psutil.pid_exists(self.pid_to_kill):
                return

            if self.time_elapsed >= self.soft_timeout and self.soft_timeout != 0:
                os.kill(self.pid_to_kill, self.soft_timeout_signal)

            if self.time_elapsed >= self.hard_timeout and self.hard_timeout != 0:
                os.kill(self.pid_to_kill, self.hard_timeout_signal)

            if self.time_elapsed >= self.critical_timeout and self.critical_timeout != 0:
                os.kill(self.pid_to_kill, self.critical_timeout_signal)

            time.sleep(self.sleep_interval)
            self.time_elapsed += self.sleep_interval

    def start(self):
        '''
        '''
        if not self.created:
            killing_loop_thread = threading.Thread(
                target=self.killing_loop,
            )
            killing_loop_thread.daemon = True
            killing_loop_thread.start()

            self.created = True

        self.stop_event.set()

    def stop(self):
        '''
        '''
        self.stop_event.clear()

    def reset(self):
        '''
        '''
        self.time_elapsed = 0.0

    def __del__(self):
        '''
        '''
        self.stop()


class RemoteKiller:
    '''
    '''
    def __init__(
        self,
        pid,
        soft_timeout,
        soft_timeout_signal,
        hard_timeout,
        hard_timeout_signal,
        critical_timeout,
        critical_timeout_signal,
    ):
        self.sleep_interval = 0.5

        self.soft_timeout = soft_timeout
        self.hard_timeout = hard_timeout
        self.critical_timeout = critical_timeout

        self.soft_timeout_signal = soft_timeout_signal
        self.hard_timeout_signal = hard_timeout_signal
        self.critical_timeout_signal = critical_timeout_signal

        self.time_elapsed = multiprocessing.Value('d', 0.0)

        self.stop_event = multiprocessing.Event()
        self.stop_event.clear()

        self.created = False

        self.pid_to_kill = pid

    def killing_loop(self):
        '''
        '''
        while self.stop_event.wait():
            if not psutil.pid_exists(self.pid_to_kill):
                return

            with self.time_elapsed.get_lock():
                if self.time_elapsed.value >= self.soft_timeout and self.soft_timeout != 0:
                    os.kill(self.pid_to_kill, self.soft_timeout_signal)

                if self.time_elapsed.value >= self.hard_timeout and self.hard_timeout != 0:
                    os.kill(self.pid_to_kill, self.hard_timeout_signal)

                if self.time_elapsed.value >= self.critical_timeout and self.critical_timeout != 0:
                    os.kill(self.pid_to_kill, self.critical_timeout_signal)

                self.time_elapsed.value += self.sleep_interval

            time.sleep(self.sleep_interval)

    def start(self):
        '''
        '''
        if not self.created:
            killing_loop_process = multiprocessing.Process(
                target=self.killing_loop,
            )
            killing_loop_process.daemon = True
            killing_loop_process.start()

            self.created = True

        self.stop_event.set()

    def stop(self):
        '''
        '''
        self.stop_event.clear()

    def reset(self):
        '''
        '''
        with self.time_elapsed.get_lock():
            self.time_elapsed.value = 0.0

    def __del__(self):
        '''
        '''
        self.stop()
