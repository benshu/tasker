import unittest
import pickle
import datetime

from .. import connectors
from .. import queue


class QueueTestCase(unittest.TestCase):
    def setUp(self):
        self.redis_connector = connectors.redis.Connector(
            host='127.0.0.1',
            port=6379,
            database=0,
        )

        self.enqueued_value = {
            'str': 'string',
            'date': datetime.datetime.utcnow(),
            'array': [1, 2, 3, 4],
        }

    def test_no_compression_queue(self):
        test_queue = queue.Queue(
            connector=self.redis_connector,
            queue_name='no_compression_queue',
            compression='',
        )

        self.queue_functionality(
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def test_gzip_queue(self):
        test_queue = queue.Queue(
            connector=self.redis_connector,
            queue_name='gzip_compression_queue',
            compression='gzip',
        )

        self.queue_functionality(
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def test_bzip2_queue(self):
        test_queue = queue.Queue(
            connector=self.redis_connector,
            queue_name='bzip2_compression_queue',
            compression='bzip2',
        )

        self.queue_functionality(
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def test_lzma_queue(self):
        test_queue = queue.Queue(
            connector=self.redis_connector,
            queue_name='lzma_compression_queue',
            compression='lzma',
        )

        self.queue_functionality(
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def queue_functionality(self, test_queue, enqueued_value):
        test_queue.flush()
        self.assertEqual(test_queue.len(), 0)

        test_queue.enqueue(
            value=enqueued_value,
        )

        self.assertEqual(test_queue.len(), 1)

        returned_value = test_queue.dequeue()

        self.assertEqual(enqueued_value, returned_value)
        self.assertEqual(test_queue.len(), 0)

        for i in range(10):
            test_queue.enqueue(
                value=enqueued_value,
            )
        self.assertEqual(test_queue.len(), 10)
        test_queue.flush()
        self.assertEqual(test_queue.len(), 0)

    def queue_pickleability(self, test_queue, enqueued_value):
        test_queue.flush()
        pickled_queue = pickle.dumps(test_queue)
        pickled_queue = pickle.loads(pickled_queue)

        self.assertEqual(test_queue.len(), 0)

        pickled_queue.enqueue(
            value=enqueued_value,
        )

        self.assertEqual(pickled_queue.len(), 1)
        self.assertEqual(test_queue.len(), 1)

        returned_value = pickled_queue.dequeue()

        self.assertEqual(enqueued_value, returned_value)
        self.assertEqual(pickled_queue.len(), 0)
        self.assertEqual(test_queue.len(), 0)

        for i in range(10):
            pickled_queue.enqueue(
                value=enqueued_value,
            )
        self.assertEqual(pickled_queue.len(), 10)
        self.assertEqual(test_queue.len(), 10)
        pickled_queue.flush()
        self.assertEqual(pickled_queue.len(), 0)
        self.assertEqual(test_queue.len(), 0)
