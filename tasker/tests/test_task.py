import unittest
import time
import logging

from .. import connector
from .. import task
from .. import queue
from .. import monitor


class EventsTestTask(task.Task):
    name = 'events_test_task'

    compression = 'none'
    timeout = 2.0
    max_tasks_per_run = 1
    max_retries = 1
    log_level = logging.CRITICAL + 10

    def init(self):
        self.succeeded = False
        self.failed = False
        self.timed_out = False
        self.retried = False
        self.max_retried = False

    def work(self, action):
        if action == 'succeeded':
            return
        elif action == 'failed':
            raise Exception
        elif action == 'timed_out':
            time.sleep(3)
        elif action == 'retried':
            self.retry()
        elif action == 'max_retried':
            self.retry()

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
        self.redis_connector = connector.redis.Connector(
            host='127.0.0.1',
            port=6379,
            database=0,
        )
        self.monitor_client = monitor.client.StatisticsClient(
            stats_server={
                'host': '127.0.0.1',
                'port': 9999,
            },
            host_name='test_host',
            worker_name='test_worker',
        )

        self.task_queue = queue.Queue(
            connector=self.redis_connector,
            queue_name='events_test_task',
            compressor='none',
            serializer='msgpack',
        )

        self.events_test_task = EventsTestTask(
            task_queue=self.task_queue,
            monitor_client=self.monitor_client,
        )

    @classmethod
    def tearDownClass(self):
        self.events_test_task.task_queue.flush()

    def test_success_event(self):
        self.events_test_task.max_tasks_per_run = 1
        self.events_test_task.max_retries = 3
        self.events_test_task.task_queue.flush()
        self.events_test_task.run(
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
        self.events_test_task.run(
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
        self.events_test_task.max_tasks_per_run = 1
        self.events_test_task.max_retries = 3
        self.events_test_task.task_queue.flush()
        self.events_test_task.run(
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
        self.events_test_task.run(
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
        self.events_test_task.run(
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
