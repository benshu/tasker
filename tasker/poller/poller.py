import time
import threading

from .. import logger


class Poller(
    threading.Thread,
):
    name = 'Poller'

    config = {
        'connector': {
            'type': 'redis',
            'params': {
                'host': 'localhost',
                'port': 6379,
                'password': 'e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
                'database': 0,
            },
        },
    }

    def __init__(
        self,
        task_queue,
        delayed_set_name,
        interval=2,
    ):
        super().__init__()

        self.task_queue = task_queue
        self.interval = interval
        self.delayed_set_name = delayed_set_name

        self._stop_event = threading.Event()
        self._stop_event.clear()

        self.logger = logger.logger.Logger(
            logger_name='poller',
        )

        self.daemon = True

    def run(
        self,
    ):
        sleep_duration = 0

        while not self._stop_event.is_set():
            if sleep_duration < self.interval:
                time.sleep(0.1)
                sleep_duration += 0.1

                continue

            try:
                self.poll_for_delayed_task()
                sleep_duration = 0
            except Exception as exception:
                self.logger.error(
                    msg=exception,
                )

    def stop(
        self,
    ):
        self._stop_event.set()

    def __del__(
        self,
    ):
        self.stop()

    def poll_for_delayed_task(
        self,
    ):
        item = self.task_queue.queue.connector.get_top_item_from_zset(
            set_name=self.delayed_set_name,
        )
        if not item:
            return

        task_data, task_time = item[0]
        if task_time > time.time():
            return

        decoded_task = self.task_queue.queue.encoder.decode(
            data=task_data,
        )
        queue_name = decoded_task['name']
        locked = self.acquire_lock_key(
            name='delayed',
            timeout=5,
        )
        if not locked:
            return

        removed_from_delayed_queue = self.task_queue.queue.connector.remove_from_zset(
            set_name=self.delayed_set_name,
            value=item,
        )
        if removed_from_delayed_queue:
            self.task_queue.enqueue(
                queue_name=queue_name,
                value=decoded_task,
            )
        self.release_lock_key(
            name='delayed',
        )

    # TODO: extract locking mechanisem to a model shared with delayed module
    def acquire_lock_key(
        self,
        name,
        ttl=None,
        timeout=None,
    ):
        try:
            sleep_interval = 0.1
            lock_key_name = '_delayed_{key_name}_lock'.format(
                key_name=name,
            )

            if timeout is not None:
                run_forever = False
                time_remaining = timeout
            else:
                run_forever = True

            while run_forever or time_remaining > 0:
                locked = self.connector.key_set(
                    key=lock_key_name,
                    value='locked',
                    ttl=ttl,
                )
                if locked:
                    return True

                time.sleep(
                    sleep_interval,
                )

                if not run_forever:
                    time_remaining -= sleep_interval

            return False
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def release_lock_key(
        self,
        name,
    ):
        try:
            self.connector.key_del(
                keys=[
                    name,
                    '_delayed_{key_name}_lock'.format(
                        key_name=name,
                    ),
                ],
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception
