import redis
import random

from . import _connector


class Connector(
    _connector.Connector,
):
    name = 'redis_cluster'

    def __init__(
        self,
        nodes,
    ):
        super().__init__()

        self.nodes = nodes

        self.connections = [
            redis.StrictRedis(
                host=node['host'],
                port=node['port'],
                password=node['password'],
                db=node['database'],
                retry_on_timeout=True,
                socket_keepalive=True,
                socket_connect_timeout=10,
                socket_timeout=60,
            )
            for node in nodes
        ]

        self.master_connection = self.connections[0]

        random.shuffle(self.connections)

    def rotate_connections(
        self,
    ):
        self.connections = self.connections[1:] + self.connections[:1]

    def key_set(
        self,
        key,
        value,
        ttl=None,
    ):
        is_new = self.master_connection.set(
            name=key,
            value=value,
            px=ttl,
            nx=True,
        )

        return is_new is True

    def key_get(
        self,
        key,
    ):
        return self.master_connection.get(
            name=key,
        )

    def key_del(
        self,
        keys,
    ):
        return self.master_connection.delete(*keys)

    def pop(
        self,
        key,
        timeout=0,
    ):
        connections = self.connections

        for connection in connections:
            value = connection.lpop(
                name=key,
            )

            if value:
                return value
            else:
                self.rotate_connections()

        return None

    def pop_bulk(
        self,
        key,
        count,
    ):
        values = []
        connections = self.connections
        current_count = count

        for connection in connections:
            pipeline = connection.pipeline()

            pipeline.lrange(key, 0, current_count - 1)
            pipeline.ltrim(key, current_count, -1)

            value = pipeline.execute()

            if len(value) != 1:
                values += value[0]
            else:
                self.rotate_connections()

                continue

            if len(values) == count:
                return values

            current_count = count - len(values)

        return values

    def push(
        self,
        key,
        value,
    ):
        push_returned_value = self.connections[0].rpush(key, value)

        self.rotate_connections()

        return push_returned_value

    def push_bulk(
        self,
        key,
        values,
    ):
        push_returned_value = self.connections[0].rpush(key, *values)

        self.rotate_connections()

        return push_returned_value

    def add_to_set(
        self,
        set_name,
        value,
    ):
        added = self.master_connection.sadd(set_name, value)

        return bool(added)

    def remove_from_set(
        self,
        set_name,
        value,
    ):
        removed = self.master_connection.srem(set_name, value)

        return bool(removed)

    def is_member_of_set(
        self,
        set_name,
        value,
    ):
        is_memeber = self.master_connection.sismember(
            name=set_name,
            value=value,
        )

        return is_memeber

    def add_to_zset(
        self,
        set_name,
        value,
        score,
    ):
        added = self.master_connection.zadd(
            set_name,
            score,
            value,
        )

        return bool(added)

    def remove_from_zset(
        self,
        set_name,
        value,
    ):
        removed = self.master_connection.zrem(
            set_name,
            value,
        )

        return bool(removed)

    def get_top_item_from_zset(
        self,
        set_name,
    ):
        item = self.master_connection.zrange(
            set_name,
            0,
            0,
            withscores=True,
        )

        return item

    def zset_length(
        self,
        set_name,
    ):
        count = self.master_connection.zcard(
            set_name,
        )

        return count

    def len(
        self,
        key,
    ):
        total_len = 0

        for connection in self.connections:
            total_len += connection.llen(
                name=key,
            )

        return total_len

    def delete(
        self,
        key,
    ):
        for connection in self.connections:
            connection.delete(key)

    def __getstate__(
        self,
    ):
        state = {
            'nodes': self.nodes,
        }

        return state

    def __setstate__(
        self,
        value,
    ):
        self.__init__(
            nodes=value['nodes'],
        )
