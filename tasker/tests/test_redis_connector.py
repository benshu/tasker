import unittest
import pickle

from .. import connector


class RedisConnectorTestCase(unittest.TestCase):
    def setUp(self):
        self.redis_connector = connector.redis.Connector(
            host='127.0.0.1',
            port=6379,
            database=0,
        )
        self.test_key = 'test_key'
        self.test_value = b'test_value'

        self.redis_connector.delete(
            key=self.test_key,
        )

    def test_connector_functionality(self):
        self.assertEqual(self.redis_connector.len(self.test_key), 0)

        self.redis_connector.push(
            key=self.test_key,
            value=self.test_value,
        )

        self.assertEqual(self.redis_connector.len(self.test_key), 1)

        returned_value = self.redis_connector.pop(
            key=self.test_key,
        )

        self.assertEqual(self.test_value, returned_value)
        self.assertEqual(self.redis_connector.len(self.test_key), 0)

        for i in range(100000):
            self.redis_connector.push(
                key=self.test_key,
                value=self.test_value,
            )
        self.assertEqual(self.redis_connector.len(self.test_key), 100000)
        self.redis_connector.delete(
            key=self.test_key,
        )
        self.assertEqual(self.redis_connector.len(self.test_key), 0)

        self.redis_connector.push_bulk(
            key=self.test_key,
            values=[self.test_value] * 100,
        )
        self.assertEqual(self.redis_connector.len(self.test_key), 100)
        values = self.redis_connector.pop_bulk(
            key=self.test_key,
            count=100,
        )
        self.assertEqual(values, [self.test_value] * 100)
        self.assertEqual(self.redis_connector.len(self.test_key), 0)

    def test_connector_pickleability(self):
        pickled_connector = pickle.dumps(self.redis_connector)
        pickled_connector = pickle.loads(pickled_connector)

        self.assertEqual(pickled_connector.len(self.test_key), 0)
        self.assertEqual(self.redis_connector.len(self.test_key), 0)

        pickled_connector.push(
            key=self.test_key,
            value=self.test_value,
        )

        self.assertEqual(pickled_connector.len(self.test_key), 1)
        self.assertEqual(self.redis_connector.len(self.test_key), 1)

        returned_value = pickled_connector.pop(
            key=self.test_key,
        )

        self.assertEqual(self.test_value, returned_value)
        self.assertEqual(pickled_connector.len(self.test_key), 0)
        self.assertEqual(self.redis_connector.len(self.test_key), 0)

        for i in range(10):
            pickled_connector.push(
                key=self.test_key,
                value=self.test_value,
            )
        self.assertEqual(pickled_connector.len(self.test_key), 10)
        self.assertEqual(self.redis_connector.len(self.test_key), 10)
        pickled_connector.delete(
            key=self.test_key,
        )
        self.assertEqual(pickled_connector.len(self.test_key), 0)
        self.assertEqual(self.redis_connector.len(self.test_key), 0)

        pickled_connector.push_bulk(
            key=self.test_key,
            values=[self.test_value] * 100,
        )
        self.assertEqual(pickled_connector.len(self.test_key), 100)
        self.assertEqual(self.redis_connector.len(self.test_key), 100)
        values = pickled_connector.pop_bulk(
            key=self.test_key,
            count=100,
        )
        self.assertEqual(values, [self.test_value] * 100)
        self.assertEqual(pickled_connector.len(self.test_key), 0)
        self.assertEqual(self.redis_connector.len(self.test_key), 0)
