import unittest
import time
import threading

from .. import connector
from .. import storage
from .. import encoder


class StorageTestCase:
    def test_lock_key(self):
        self.storage.release_lock_key(
            name=self.test_key,
        )
        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second={},
        )

        time_acquired = time.time()
        acquired = self.storage.acquire_lock_key(
            name=self.test_key,
            ttl=2000,
        )
        self.assertTrue(acquired)

        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second='locked',
        )

        acquired = self.storage.acquire_lock_key(
            name=self.test_key,
        )
        self.assertTrue(acquired)
        time_released = time.time()

        lock_time = time_released - time_acquired
        self.assertTrue(
            2.1 > lock_time > 1.9
        )

        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second='locked',
        )

        self.storage.release_lock_key(
            name=self.test_key,
        )
        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second={},
        )

        self.storage.acquire_lock_key(
            name=self.test_key,
        )
        self.assertTrue(acquired)

        timer = threading.Timer(
            2,
            self.storage.release_lock_key,
            args=(
                self.test_key,
            )
        )
        timer.start()

        acquired = self.storage.acquire_lock_key(
            name=self.test_key,
            timeout=3,
        )
        self.assertTrue(acquired)

        acquired = self.storage.acquire_lock_key(
            name=self.test_key,
            timeout=2,
        )
        self.assertFalse(acquired)

    def test_functions(self):
        self.storage.release_lock_key(
            name=self.test_key,
        )
        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second={},
        )

        acquired = self.storage.acquire_lock_key(
            name=self.test_key,
        )
        self.assertTrue(acquired)

        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second='locked',
        )

        self.storage.release_lock_key(
            name=self.test_key,
        )
        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second={},
        )


class SingleRedisStorageTestCase(
    StorageTestCase,
    unittest.TestCase,
):
    def setUp(self):
        self.redis_connector = connector.redis.Connector(
            host='127.0.0.1',
            port=6379,
            password='e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
            database=0,
        )

        self.storage = storage.storage.Storage(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_key = 'test_key'
        self.test_lock_key = '_storage_{key_name}_lock'.format(
            key_name=self.test_key,
        )


class RedisClusterStorageTestCase(
    StorageTestCase,
    unittest.TestCase,
):
    def setUp(self):
        self.redis_connector = connector.redis_cluster.Connector(
            nodes=[
                {
                    'host': '127.0.0.1',
                    'port': 6379,
                    'password': 'e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
                    'database': 0,
                },
                {
                    'host': '127.0.0.1',
                    'port': 6380,
                    'password': 'e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
                    'database': 0,
                },
            ]
        )

        self.storage = storage.storage.Storage(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_key = 'test_key'
        self.test_lock_key = '_storage_{key_name}_lock'.format(
            key_name=self.test_key,
        )
