import unittest
import time
import logging

from .. import worker


class EventsTestWorker(
    worker.Worker,
):
    name = 'events_test_worker'

    config = worker.Worker.config.copy()
    config.update(
        {
            'timeouts': {
                'soft_timeout': 2.0,
                'hard_timeout': 0.0,
                'critical_timeout': 0.0,
                'global_timeout': 0.0,
            },
            'executor': {
                'type': 'serial',
            },
            'max_tasks_per_run': 1,
            'max_retries': 1,
            'tasks_per_transaction': 1,
            'report_completion': True,
            'heartbeat_interval': 0.0,
        },
    )

    def init(
        self,
    ):
        self.succeeded = False
        self.failed = False
        self.timed_out = False
        self.retried = False
        self.requeued = False
        self.max_retried = False

        self.requeue_count = 0

        self.logger.logger.setLevel(
            logging.CRITICAL + 10,
        )

    def work(
        self,
        action,
    ):
        if action == 'succeeded':
            return 'success'
        elif action == 'failed':
            raise Exception
        elif action == 'timed_out':
            time.sleep(3)
        elif action == 'retried':
            self.retry()
        elif action == 'requeued':
            self.requeue_count += 1
            if self.requeue_count > 3:
                return

            self.requeue()
        elif action == 'max_retried':
            self.retry()
        elif action == 'report_completion':
            time.sleep(2)

    def on_success(
        self,
        returned_value,
        args,
        kwargs,
    ):
        self.succeeded = True

    def on_failure(
        self,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        self.failed = True

    def on_timeout(
        self,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        self.timed_out = True

    def on_retry(
        self,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        self.retried = True

    def on_requeue(
        self,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        self.requeued = True

    def on_max_retries(
        self,
        exception,
        exception_traceback,
        args,
        kwargs,
    ):
        self.max_retried = True


class DelayedWorkerTestCase(
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.worker = EventsTestWorker()
        self.worker.init()
        self.worker.init_worker()

    def test_delayed_task_push(
        self,
    ):
        self.worker.apply_async_one(
            time_to_enqueue=time.time() + 3,
            kwargs={
                'action': 'success',
            },
        )
        time.sleep(10)
