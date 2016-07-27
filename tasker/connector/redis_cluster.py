import rediscluster

from . import _connector


class Connector(_connector.Connector):
    '''
    '''
    name = 'redis_cluster'

    def __init__(self, startup_nodes):
        super().__init__()

        self.startup_nodes = startup_nodes

        self.connection = rediscluster.StrictRedisCluster(
            startup_nodes=startup_nodes,
            retry_on_timeout=True,
            socket_keepalive=True,
            socket_connect_timeout=10,
            socket_timeout=60,
        )

    def pop(self, key):
        '''
        '''
        value = self.connection.blpop(
            keys=[key],
        )

        if value is None:
            return None
        else:
            return value[1]

    def pop_bulk(self, key, count):
        '''
        '''
        pipeline = self.connection.pipeline()

        pipeline.lrange(key, 0, count - 1)
        pipeline.ltrim(key, count, -1)

        value = pipeline.execute()

        if len(value) == 1:
            return []
        else:
            return value[0]

    def push(self, key, value):
        '''
        '''
        return self.connection.rpush(key, value)

    def push_bulk(self, key, values):
        '''
        '''
        return self.connection.rpush(key, *values)

    def add_to_set(self, set_name, value):
        '''
        '''
        added = self.connection.sadd(set_name, value)

        return bool(added)

    def remove_from_set(self, set_name, value):
        '''
        '''
        removed = self.connection.srem(set_name, value)

        return bool(removed)

    def is_member_of_set(self, set_name, value):
        '''
        '''
        is_memeber = self.connection.sismember(
            name=set_name,
            value=value,
        )

        return is_memeber

    def len(self, key):
        '''
        '''
        return self.connection.llen(
            name=key,
        )

    def delete(self, key):
        '''
        '''
        return self.connection.delete(key)

    def __getstate__(self):
        '''
        '''
        state = {
            'startup_nodes': self.startup_nodes,
        }

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            startup_nodes=value['startup_nodes'],
        )
