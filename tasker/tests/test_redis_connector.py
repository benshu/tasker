import unittest
import pickle
import time

from .. import connector


class RedisConnectorTestCase(
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.redis_connector = connector.redis.Connector(
            host='127.0.0.1',
            port=6379,
            password='e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
            database=0,
        )
        self.test_key = 'test_key'
        self.set_name = 'test_set'
        self.zset_name = 'zset_name'
        self.test_value = b'test_value'
        self.test_set_value = b'test_value'

        self.redis_connector.delete(
            key=self.test_key,
        )
        self.redis_connector.delete(
            key=self.zset_name,
        )

    def test_add_to_zset(
        self,
    ):
        self.assertEqual(
            self.redis_connector.zset_length(self.test_key),
            0,
        )
        delay = 10000
        time_in_future = time.time() + delay

        self.redis_connector.add_to_zset(
            set_name=self.zset_name,
            value=self.test_value,
            score=time_in_future,
        )

        self.assertEqual(
            self.redis_connector.zset_length(
                self.zset_name,
            ),
            1,
        )

    def test_remove_from_zset(
        self,
    ):
        self.assertEqual(
            self.redis_connector.zset_length(self.zset_name),
            0,
        )

        delay = 10000
        time_in_future = time.time() + delay

        self.redis_connector.add_to_zset(
            set_name=self.zset_name,
            value=self.test_value,
            score=time_in_future,
        )

        self.assertEqual(
            self.redis_connector.zset_length(self.zset_name),
            1,
            msg='Expected single item in queue before testing remove_from_zset',
        )

        returned_value = self.redis_connector.remove_from_zset(
            set_name=self.zset_name,
            value=self.test_value,
        )

        self.assertTrue(
            returned_value,
        )
        self.assertEqual(
            self.redis_connector.zset_length(self.zset_name),
            0,
        )

    def test_get_top_item_from_zset(
        self,
    ):
        self.assertEqual(
            self.redis_connector.zset_length(self.zset_name),
            0,
        )

        delay = 10000
        time_in_future = time.time() + delay

        self.redis_connector.add_to_zset(
            set_name=self.zset_name,
            value=self.test_value,
            score=time_in_future,
        )

        self.assertEqual(
            self.redis_connector.zset_length(self.zset_name),
            1,
            msg='Expected single item in queue before testing get_top_item_from_zset',
        )
        returned_value = self.redis_connector.get_top_item_from_zset(
            set_name=self.zset_name,
        )

        self.assertTrue(
            returned_value,
            self.test_value,
        )
        self.assertEqual(
            self.redis_connector.zset_length(self.zset_name),
            1,
            msg='Expected single item in queue after testing get_top_item_from_zset',
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

        added = self.redis_connector.add_to_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(added)
        added = self.redis_connector.add_to_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertFalse(added)
        is_member = self.redis_connector.is_member_of_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(is_member)
        removed = self.redis_connector.remove_from_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(removed)
        removed = self.redis_connector.remove_from_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertFalse(removed)

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

        added = pickled_connector.add_to_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(added)
        added = pickled_connector.add_to_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertFalse(added)
        is_member = pickled_connector.is_member_of_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(is_member)
        removed = pickled_connector.remove_from_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(removed)
        removed = pickled_connector.remove_from_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertFalse(removed)
