import time
import threading

from .. import logger


class Reporter(threading.Thread):
    '''
    '''
    def __init__(self, pipe, interval):
        super().__init__()

        self.pipe = pipe
        self.interval = interval

        self._stop_event = threading.Event()
        self._stop_event.set()

        self.logger = logger.logger.Logger(
            logger_name='supervisor_reporter',
        )

        self.daemon = True

    def run(self):
        '''
        '''
        sleep_duration = 0

        while self._stop_event.is_set():
            if sleep_duration < self.interval:
                time.sleep(0.1)
                sleep_duration += 0.1

                continue

            try:
                self.pipe.send(b'heartbeat')

                sleep_duration = 0
            except Exception as exception:
                self.logger.error(
                    msg=exception,
                )

    def stop(self):
        '''
        '''
        self._stop_event.clear()
