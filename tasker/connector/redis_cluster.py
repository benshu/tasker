import rediscluster

from . import _connector


class Connector(_connector.Connector):
    '''
    '''
    name = 'redis_cluster'

    def __init__(self, startup_nodes):
        self.startup_nodes = startup_nodes

        self.connection = rediscluster.StrictRedisCluster(
            startup_nodes=startup_nodes,
            retry_on_timeout=True,
            socket_keepalive=False,
            socket_connect_timeout=10,
            socket_timeout=60,
        )

    def pop(self, key, timeout=0):
        '''
        '''
        value = self.connection.blpop(
            keys=[key],
            timeout=timeout,
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
        pipeline = self.connection.pipeline()

        for value in values:
            pipeline.rpush(key, value)

        return pipeline.execute()

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
