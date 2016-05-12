import time
import threading
import os


class Killer:
    '''
    '''
    def __init__(self, soft_timeout, soft_timeout_signal, hard_timeout, hard_timeout_signal):
        super().__init__()

        self.sleep_interval = 1.0

        self.soft_timeout = soft_timeout
        self.hard_timeout = hard_timeout

        self.soft_timeout_signal = soft_timeout_signal
        self.hard_timeout_signal = hard_timeout_signal

        self.time_elapsed = 0.0

        self.stop_event = threading.Event()
        self.stop_event.clear()
        self.kill_event = threading.Event()
        self.kill_event.clear()

    def killing_loop(self):
        '''
        '''
        while self.stop_event.wait():
            if self.kill_event.is_set():
                return

            if self.time_elapsed >= self.soft_timeout and self.soft_timeout != 0:
                print('kill soft')
                os.kill(os.getpid(), self.soft_timeout_signal)

            if self.time_elapsed >= self.hard_timeout and self.hard_timeout != 0:
                print('kill hard')
                os.kill(os.getpid(), self.hard_timeout_signal)

            time.sleep(self.sleep_interval)
            self.time_elapsed += self.sleep_interval

    def create(self):
        '''
        '''
        self.killing_loop_thread = threading.Thread(
            target=self.killing_loop,
        )
        self.killing_loop_thread.start()

    def start(self):
        '''
        '''
        self.stop_event.set()

    def stop(self):
        '''
        '''
        self.stop_event.clear()

    def reset(self):
        '''
        '''
        self.time_elapsed = 0.0

    def destroy(self):
        '''
        '''
        self.stop_event.set()
        self.kill_event.set()
