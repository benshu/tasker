import unittest
import time
import socket
import multiprocessing
import logging

from .. import worker


class EventsTestWorker(worker.Worker):
    name = 'events_test_worker'
    config = {
        'compressor': 'dummy',
        'serializer': 'pickle',
        'monitoring': {
            'host_name': socket.gethostname(),
            'stats_server': {
                'host': '',
                'port': 9999,
            }
        },
        'connector': {
            'type': 'redis',
            'params': {
                'host': 'localhost',
                'port': 6379,
                'database': 0,
            },
        },
        'soft_timeout': 2.0,
        'hard_timeout': 0.0,
        'global_timeout': 0.0,
        'max_tasks_per_run': 1,
        'max_retries': 1,
        'tasks_per_transaction': 10,
        'report_completion': True,
        'heartbeat_interval': 10.0,
    }

    def init(self):
        self.succeeded = False
        self.failed = False
        self.timed_out = False
        self.retried = False
        self.max_retried = False

        self.logger.logger.setLevel(logging.CRITICAL + 10)

    def work(self, action):
        if action == 'succeeded':
            return 'success'
        elif action == 'failed':
            raise Exception
        elif action == 'timed_out':
            time.sleep(3)
        elif action == 'retried':
            self.retry()
        elif action == 'max_retried':
            self.retry()
        elif action == 'report_completion':
            time.sleep(2)

    def on_success(self, returned_value, args, kwargs):
        self.succeeded = True

    def on_failure(self, exception, exception_traceback, args, kwargs):
        self.failed = True

    def on_timeout(self, exception, exception_traceback, args, kwargs):
        self.timed_out = True

    def on_retry(self, args, kwargs):
        self.retried = True

    def on_max_retries(self, exception, exception_traceback, args, kwargs):
        self.max_retried = True


class TaskTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.events_test_worker = EventsTestWorker()

    @classmethod
    def tearDownClass(self):
        self.events_test_worker.purge_tasks()

    def test_success_event(self):
        self.events_test_worker.max_tasks_per_run = 1
        self.events_test_worker.max_retries = 3
        self.events_test_worker.purge_tasks()
        self.events_test_worker.apply_async_one(
            action='succeeded',
        )
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            1,
        )

        self.events_test_worker.work_loop()
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            0,
        )
        self.assertTrue(
            getattr(
                self.events_test_worker,
                'succeeded',
            )
        )

    def test_failure_event(self):
        self.events_test_worker.max_tasks_per_run = 1
        self.events_test_worker.max_retries = 3
        self.events_test_worker.purge_tasks()
        self.events_test_worker.apply_async_one(
            action='failed',
        )
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            1,
        )

        self.events_test_worker.work_loop()
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            0,
        )
        self.assertTrue(
            getattr(
                self.events_test_worker,
                'failed',
            )
        )

    def test_time_out_event(self):
        self.events_test_worker.config['soft_timeout'] = 2.0
        self.events_test_worker.config['max_tasks_per_run'] = 1
        self.events_test_worker.config['max_retries'] = 3
        self.events_test_worker.purge_tasks()
        self.events_test_worker.apply_async_one(
            action='timed_out',
        )
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            1,
        )

        self.events_test_worker.work_loop()
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            0,
        )
        self.assertTrue(
            getattr(
                self.events_test_worker,
                'timed_out',
            )
        )

    def test_retry_event(self):
        self.events_test_worker.config['max_tasks_per_run'] = 1
        self.events_test_worker.config['max_retries'] = 3
        self.events_test_worker.purge_tasks()
        self.events_test_worker.apply_async_one(
            action='retried',
        )
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            1,
        )

        self.events_test_worker.work_loop()
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            1,
        )
        self.assertTrue(
            getattr(
                self.events_test_worker,
                'retried',
            )
        )

    def test_max_retries_event(self):
        self.events_test_worker.config['max_tasks_per_run'] = 3
        self.events_test_worker.config['max_retries'] = 2
        self.events_test_worker.purge_tasks()
        self.events_test_worker.apply_async_one(
            action='max_retried',
        )
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            1,
        )

        self.events_test_worker.work_loop()
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            0,
        )
        self.assertTrue(
            getattr(
                self.events_test_worker,
                'max_retried',
            )
        )

    def test_completion_report(self):
        self.events_test_worker.soft_timeout = 10
        self.events_test_worker.max_retries = 2
        self.events_test_worker.purge_tasks()
        task = self.events_test_worker.apply_async_one(
            action='report_completion',
        )
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            1,
        )

        before = time.time()
        worker_process = multiprocessing.Process(target=self.events_test_worker.work_loop)
        worker_process.start()

        time.sleep(0.5)
        self.assertEqual(
            self.events_test_worker.number_of_enqueued_tasks(),
            0,
        )

        self.events_test_worker.wait_task_finished(task)

        after = time.time()
        self.assertTrue(2 <= after - before <= 3)
