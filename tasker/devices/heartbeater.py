import time
import threading


class Heartbeater(threading.Thread):
    '''
    '''
    def __init__(self, monitor_client, interval):
        super().__init__()

        self.monitor_client = monitor_client
        self.interval = interval

        self._stop_event = threading.Event()
        self._stop_event.set()

    def run(self):
        '''
        '''
        sleep_duration = 0

        while self._stop_event.is_set():
            if sleep_duration < self.interval:
                time.sleep(0.5)
                sleep_duration += 0.5

                continue

            try:
                self.monitor_client.send_heartbeat()

                sleep_duration = 0
            except Exception:
                pass

    def stop(self):
        '''
        '''
        self._stop_event.clear()
