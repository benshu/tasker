import uuid
import unittest
import pickle
import datetime

from .. import connector
from .. import queue
from .. import encoder


class QueueTestCase(unittest.TestCase):
    def setUp(self):
        self.redis_connector = connector.redis.Connector(
            host='127.0.0.1',
            port=6379,
            password='e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
            database=0,
        )

        self.enqueued_value = {
            'str': 'string',
            'date': datetime.datetime.utcnow().timestamp(),
            'array': [1, 2, 3, 4],
        }

        self.test_set_value = uuid.uuid4()

    def test_no_compression_queue(self):
        test_queue = queue.regular.Queue(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            queue_name='no_compression_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            queue_name='no_compression_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def test_zlib_queue(self):
        test_queue = queue.regular.Queue(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='zlib',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            queue_name='zlib_compression_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            queue_name='zlib_compression_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def test_gzip_queue(self):
        test_queue = queue.regular.Queue(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='gzip',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            queue_name='gzip_compression_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            queue_name='gzip_compression_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def test_bzip2_queue(self):
        test_queue = queue.regular.Queue(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='bzip2',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            queue_name='bzip2_compression_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            queue_name='bzip2_compression_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def test_lzma_queue(self):
        test_queue = queue.regular.Queue(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='lzma',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            queue_name='lzma_compression_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            queue_name='lzma_compression_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def test_pickle_queue(self):
        test_queue = queue.regular.Queue(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            queue_name='pickle_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            queue_name='pickle_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def test_msgpack_queue(self):
        test_queue = queue.regular.Queue(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='msgpack',
            ),
        )

        self.queue_functionality(
            queue_name='pickle_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            queue_name='pickle_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def test_msgpack_compressed_queue(self):
        test_queue = queue.regular.Queue(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='zlib',
                serializer_name='msgpack',
            ),
        )

        self.queue_functionality(
            queue_name='pickle_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            queue_name='pickle_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def test_pickle_compressed_queue(self):
        test_queue = queue.regular.Queue(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='zlib',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            queue_name='pickle_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )
        self.queue_pickleability(
            queue_name='pickle_queue',
            test_queue=test_queue,
            enqueued_value=self.enqueued_value,
        )

    def queue_functionality(self, queue_name, test_queue, enqueued_value):
        test_queue.flush(
            queue_name=queue_name,
        )
        self.assertEqual(test_queue.len(queue_name=queue_name), 0)

        test_queue.enqueue(
            queue_name=queue_name,
            value=enqueued_value,
        )

        self.assertEqual(test_queue.len(queue_name=queue_name), 1)

        returned_value = test_queue.dequeue(
            queue_name=queue_name,
        )

        self.assertEqual(enqueued_value, returned_value)
        self.assertEqual(test_queue.len(queue_name=queue_name), 0)

        for i in range(10):
            test_queue.enqueue(
                queue_name=queue_name,
                value=enqueued_value,
            )
        self.assertEqual(test_queue.len(queue_name=queue_name), 10)
        test_queue.flush(
            queue_name=queue_name,
        )
        self.assertEqual(test_queue.len(queue_name=queue_name), 0)

        test_queue.enqueue_bulk(
            queue_name=queue_name,
            values=[enqueued_value] * 100,
        )
        self.assertEqual(test_queue.len(queue_name=queue_name), 100)
        values = test_queue.dequeue_bulk(
            queue_name=queue_name,
            count=100,
        )
        self.assertEqual(test_queue.len(queue_name=queue_name), 0)
        self.assertEqual(values, [enqueued_value] * 100)
        test_queue.flush(
            queue_name=queue_name,
        )
        self.assertEqual(test_queue.len(queue_name=queue_name), 0)

        added = test_queue.add_result(
            queue_name=queue_name,
            value=self.test_set_value,
        )
        self.assertTrue(added)
        added = test_queue.add_result(
            queue_name=queue_name,
            value=self.test_set_value,
        )
        self.assertFalse(added)
        is_member = test_queue.has_result(
            queue_name=queue_name,
            value=self.test_set_value,
        )
        self.assertTrue(is_member)
        removed = test_queue.remove_result(
            queue_name=queue_name,
            value=self.test_set_value,
        )
        self.assertTrue(removed)
        removed = test_queue.remove_result(
            queue_name=queue_name,
            value=self.test_set_value,
        )
        self.assertFalse(removed)

    def queue_pickleability(self, queue_name, test_queue, enqueued_value):
        test_queue.flush(
            queue_name=queue_name,
        )
        pickled_queue = pickle.dumps(test_queue)
        pickled_queue = pickle.loads(pickled_queue)

        self.assertEqual(test_queue.len(queue_name=queue_name), 0)

        pickled_queue.enqueue(
            queue_name=queue_name,
            value=enqueued_value,
        )

        self.assertEqual(pickled_queue.len(queue_name=queue_name), 1)
        self.assertEqual(test_queue.len(queue_name=queue_name), 1)

        returned_value = pickled_queue.dequeue(
            queue_name=queue_name,
        )

        self.assertEqual(enqueued_value, returned_value)
        self.assertEqual(pickled_queue.len(queue_name=queue_name), 0)
        self.assertEqual(test_queue.len(queue_name=queue_name), 0)

        for i in range(10):
            pickled_queue.enqueue(
                queue_name=queue_name,
                value=enqueued_value,
            )
        self.assertEqual(pickled_queue.len(queue_name=queue_name), 10)
        self.assertEqual(test_queue.len(queue_name=queue_name), 10)
        pickled_queue.flush(queue_name=queue_name)
        self.assertEqual(pickled_queue.len(queue_name=queue_name), 0)
        self.assertEqual(test_queue.len(queue_name=queue_name), 0)

        pickled_queue.enqueue_bulk(
            queue_name=queue_name,
            values=[enqueued_value] * 100,
        )
        self.assertEqual(pickled_queue.len(queue_name=queue_name), 100)
        self.assertEqual(test_queue.len(queue_name=queue_name), 100)
        values = pickled_queue.dequeue_bulk(
            queue_name=queue_name,
            count=100,
        )
        self.assertEqual(pickled_queue.len(queue_name=queue_name), 0)
        self.assertEqual(test_queue.len(queue_name=queue_name), 0)
        self.assertEqual(values, [enqueued_value] * 100)
        pickled_queue.flush(queue_name=queue_name)
        self.assertEqual(pickled_queue.len(queue_name=queue_name), 0)
        self.assertEqual(test_queue.len(queue_name=queue_name), 0)

        added = pickled_queue.add_result(
            queue_name=queue_name,
            value=self.test_set_value,
        )
        self.assertTrue(added)
        added = pickled_queue.add_result(
            queue_name=queue_name,
            value=self.test_set_value,
        )
        self.assertFalse(added)
        is_member = pickled_queue.has_result(
            queue_name=queue_name,
            value=self.test_set_value,
        )
        self.assertTrue(is_member)
        removed = pickled_queue.remove_result(
            queue_name=queue_name,
            value=self.test_set_value,
        )
        self.assertTrue(removed)
        removed = pickled_queue.remove_result(
            queue_name=queue_name,
            value=self.test_set_value,
        )
        self.assertFalse(removed)
