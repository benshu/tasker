import unittest
import datetime
import threading
import time

from .. import task_queue
from .. import connector
from .. import queue
from .. import encoder


class RedisTaskQueueTestCase(unittest.TestCase):
    def setUp(self):
        redis_connector = connector.redis.Connector(
            host='127.0.0.1',
            port=6379,
            database=0,
        )

        test_queue = queue.regular.Queue(
            connector=redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_task_queue = task_queue.TaskQueue(
            queue=test_queue,
        )

    def test_purge_tasks(self):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        self.assertEqual(
            self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            0,
        )
        task = self.test_task_queue.craft_task(
            task_name='test_task',
        )
        self.test_task_queue.apply_async_one(task)
        self.assertEqual(
            self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            1,
        )
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        self.assertEqual(
            self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            0,
        )

    def test_number_of_enqueued_tasks(self):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        self.assertEqual(
            self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            0,
        )
        task = self.test_task_queue.craft_task(
            task_name='test_task',
        )
        self.test_task_queue.apply_async_one(task)
        self.assertEqual(
            self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            1,
        )
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        self.assertEqual(
            self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            0,
        )

        self.test_task_queue.apply_async_many([task] * 100)
        self.assertEqual(
            self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            100,
        )
        self.test_task_queue.apply_async_many([task] * 1000)
        self.assertEqual(
            self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            1100,
        )
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        self.assertEqual(
            self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            0,
        )

    def test_craft_task(self):
        task = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={},
            report_completion=False,
        )
        current_date = datetime.datetime.utcnow().timestamp()
        date = task.pop('date')
        self.assertAlmostEqual(
            date / (10 ** 8),
            current_date / (10 ** 8),
        )
        self.assertEqual(
            task,
            {
                'name': 'test_task',
                'args': (),
                'kwargs': {},
                'run_count': 0,
                'completion_key': None,
            }
        )

        task = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(1, 2, 3),
            kwargs={
                'a': 1,
                'b': 2,
            },
            report_completion=True,
        )
        current_date = datetime.datetime.utcnow().timestamp()
        date = task.pop('date')
        self.assertAlmostEqual(
            date / (10 ** 8),
            current_date / (10 ** 8),
        )
        completion_key = task.pop('completion_key')
        self.assertNotEqual(completion_key, None)
        self.assertEqual(
            task,
            {
                'name': 'test_task',
                'args': (1, 2, 3),
                'kwargs': {
                    'a': 1,
                    'b': 2,
                },
                'run_count': 0,
            }
        )

    def test_report_complete(self):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={},
            report_completion=True,
        )
        completion_key = task['completion_key']
        self.assertTrue(
            self.test_task_queue.queue.has_result(
                queue_name='test_task',
                value=completion_key,
            )
        )
        self.test_task_queue.report_complete(
            task=task,
        )
        self.assertFalse(
            self.test_task_queue.queue.has_result(
                queue_name='test_task',
                value=completion_key,
            )
        )

    def test_wait_task_finished(self):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={},
            report_completion=True,
        )
        report_complete_timer = threading.Timer(
            interval=2.0,
            function=self.test_task_queue.report_complete,
            args=(task,),
        )
        report_complete_timer.start()

        before = time.time()
        self.test_task_queue.wait_task_finished(
            task=task,
        )
        after = time.time()
        self.assertTrue(3.0 > after - before > 2.0)

    def test_wait_queue_empty(self):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={},
            report_completion=True,
        )
        self.test_task_queue.apply_async_one(task)
        purge_tasks_timer = threading.Timer(
            interval=2.0,
            function=self.test_task_queue.purge_tasks,
            args=('test_task',),
        )
        purge_tasks_timer.start()

        before = time.time()
        self.test_task_queue.wait_queue_empty(
            task_name='test_task',
        )
        after = time.time()
        self.assertTrue(3.5 > after - before > 3.0)

    def test_apply_async_one(self):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task_one = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(1,),
            kwargs={},
            report_completion=False,
        )
        task_two = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={
                'a': 1,
            },
            report_completion=True,
        )
        task_three = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={},
            report_completion=True,
        )
        self.test_task_queue.apply_async_one(task_one)
        self.test_task_queue.apply_async_one(task_two)
        self.test_task_queue.apply_async_one(task_three)
        task_one_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
        )
        task_two_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
        )
        task_three_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
        )
        self.assertEqual(task_one, task_one_test)
        self.assertEqual(task_two, task_two_test)
        self.assertEqual(task_three, task_three_test)

        self.assertTrue(
            self.test_task_queue.queue.has_result(
                queue_name='test_task',
                value=task_two['completion_key'],
            )
        )
        self.assertTrue(
            self.test_task_queue.queue.has_result(
                queue_name='test_task',
                value=task_three['completion_key'],
            )
        )

    def test_apply_async_many(self):
        self.test_task_queue.purge_tasks(
            task_name='test_task_one',
        )
        self.test_task_queue.purge_tasks(
            task_name='test_task_two',
        )
        task_one = self.test_task_queue.craft_task(
            task_name='test_task_one',
            args=(1,),
            kwargs={},
            report_completion=False,
        )
        task_two = self.test_task_queue.craft_task(
            task_name='test_task_one',
            args=(),
            kwargs={
                'a': 1,
            },
            report_completion=True,
        )
        task_three = self.test_task_queue.craft_task(
            task_name='test_task_two',
            args=(),
            kwargs={},
            report_completion=True,
        )
        self.test_task_queue.apply_async_many(
            [
                task_one,
                task_two,
                task_three,
            ]
        )
        task_one_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task_one',
        )
        task_two_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task_one',
        )
        task_three_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task_two',
        )
        self.assertEqual(task_one, task_one_test)
        self.assertEqual(task_two, task_two_test)
        self.assertEqual(task_three, task_three_test)

        self.assertTrue(
            self.test_task_queue.queue.has_result(
                queue_name='test_task_one',
                value=task_two['completion_key'],
            )
        )
        self.assertTrue(
            self.test_task_queue.queue.has_result(
                queue_name='test_task_two',
                value=task_three['completion_key'],
            )
        )

    def test_get_tasks(self):
        self.test_task_queue.purge_tasks(
            task_name='test_task_one',
        )
        self.test_task_queue.purge_tasks(
            task_name='test_task_two',
        )
        task_one = self.test_task_queue.craft_task(
            task_name='test_task_one',
            args=(1,),
            kwargs={},
            report_completion=False,
        )
        task_two = self.test_task_queue.craft_task(
            task_name='test_task_one',
            args=(),
            kwargs={
                'a': 1,
            },
            report_completion=True,
        )
        task_three = self.test_task_queue.craft_task(
            task_name='test_task_two',
            args=(),
            kwargs={},
            report_completion=True,
        )
        self.test_task_queue.apply_async_many(
            [
                task_one,
                task_two,
                task_three,
            ]
        )
        tasks_one = self.test_task_queue.get_tasks(
            task_name='test_task_one',
            num_of_tasks=3,
        )
        tasks_two = self.test_task_queue.get_tasks(
            task_name='test_task_two',
            num_of_tasks=1,
        )
        self.assertTrue(task_one in tasks_one)
        self.assertTrue(task_two in tasks_one)
        self.assertTrue(task_three in tasks_two)

    def test_retry(self):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task_one = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(1,),
            kwargs={},
            report_completion=False,
        )
        self.assertEqual(task_one['run_count'], 0)
        self.test_task_queue.apply_async_one(task_one)
        task_one = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
        )
        self.test_task_queue.retry(task_one)
        task_one = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
        )
        self.assertEqual(task_one['run_count'], 1)


class RedisClusterTaskQueueTestCase(RedisTaskQueueTestCase):
    def setUp(self):
        redis_cluster_connector = connector.redis_cluster.Connector(
            nodes=[
                {
                    'host': '127.0.0.1',
                    'port': 6379,
                    'database': 0,
                }
            ]
        )

        test_queue = queue.regular.Queue(
            connector=redis_cluster_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_task_queue = task_queue.TaskQueue(
            queue=test_queue,
        )
