import unittest
import time
import logging
import threading

from .. import task


class EventsTestTask(task.Task):
    name = 'events_test_task'

    compression = 'dummy'
    timeout = 2.0
    max_tasks_per_run = 1
    max_retries = 1
    log_level = logging.CRITICAL + 10
    report_completion = True
    monitoring = {}

    def init(self):
        self.succeeded = False
        self.failed = False
        self.timed_out = False
        self.retried = False
        self.max_retried = False

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
            time.sleep(5)

    def on_success(self, returned_value, args, kwargs):
        self.succeeded = True

    def on_failure(self, exception, args, kwargs):
        self.failed = True

    def on_timeout(self, exception, args, kwargs):
        self.timed_out = True

    def on_retry(self, args, kwargs):
        self.retried = True

    def on_max_retries(self, args, kwargs):
        self.max_retried = True


class TaskTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.events_test_task = EventsTestTask()

    @classmethod
    def tearDownClass(self):
        self.events_test_task.task_queue.flush()

    def test_success_event(self):
        self.events_test_task.max_tasks_per_run = 1
        self.events_test_task.max_retries = 3
        self.events_test_task.task_queue.flush()
        self.events_test_task.apply_async_one(
            action='succeeded',
        )
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            1,
        )

        self.events_test_task.work_loop()
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            0,
        )
        self.assertTrue(
            getattr(
                self.events_test_task,
                'succeeded',
            )
        )

    def test_failure_event(self):
        self.events_test_task.max_tasks_per_run = 1
        self.events_test_task.max_retries = 3
        self.events_test_task.task_queue.flush()
        self.events_test_task.apply_async_one(
            action='failed',
        )
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            1,
        )

        self.events_test_task.work_loop()
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            0,
        )
        self.assertTrue(
            getattr(
                self.events_test_task,
                'failed',
            )
        )

    def test_time_out_event(self):
        self.events_test_task.timeout = 2.0
        self.events_test_task.max_tasks_per_run = 1
        self.events_test_task.max_retries = 3
        self.events_test_task.task_queue.flush()
        self.events_test_task.apply_async_one(
            action='timed_out',
        )
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            1,
        )

        self.events_test_task.work_loop()
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            0,
        )
        self.assertTrue(
            getattr(
                self.events_test_task,
                'timed_out',
            )
        )

    def test_retry_event(self):
        self.events_test_task.max_tasks_per_run = 1
        self.events_test_task.max_retries = 3
        self.events_test_task.task_queue.flush()
        self.events_test_task.apply_async_one(
            action='retried',
        )
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            1,
        )

        self.events_test_task.work_loop()
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            1,
        )
        self.assertTrue(
            getattr(
                self.events_test_task,
                'retried',
            )
        )

    def test_max_retries_event(self):
        self.events_test_task.max_tasks_per_run = 3
        self.events_test_task.max_retries = 2
        self.events_test_task.task_queue.flush()
        self.events_test_task.apply_async_one(
            action='max_retried',
        )
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            1,
        )

        self.events_test_task.work_loop()
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            0,
        )
        self.assertTrue(
            getattr(
                self.events_test_task,
                'max_retried',
            )
        )

    def test_completion_report(self):
        self.events_test_task.timeout = 10
        self.events_test_task.max_retries = 2
        self.events_test_task.task_queue.flush()
        task = self.events_test_task.apply_async_one(
            action='report_completion',
        )
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            1,
        )

        before = time.time()
        thread = threading.Thread(target=self.events_test_task.work_loop)
        thread.start()

        time.sleep(0.5)
        self.assertEqual(
            self.events_test_task.task_queue.len(),
            0,
        )

        self.events_test_task.wait_task_finished(task)

        after = time.time()
        self.assertTrue(after - before > 5)
        self.assertTrue(after - before < 6)
