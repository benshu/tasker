import redis


class Connector:
    '''
    '''
    def __init__(self, host, port, database):
        self.host = host
        self.port = port
        self.database = database

    def connect(self):
        '''
        '''
        self.connection = redis.StrictRedis(
            host=self.host,
            port=self.port,
            db=self.database,
            retry_on_timeout=True,
            socket_keepalive=True,
            socket_connect_timeout=10,
            socket_timeout=60,
        )

    def disconnect(self):
        '''
        '''
        pass

    def pop(self, key, timeout=0):
        '''
        '''
        value = self.connection.blpop(
            keys=[key],
            timeout=timeout,
        )

        return value

    def push(self, key, value):
        '''
        '''
        return self.connection.rpush(key, value)

    def len(self, key):
        '''
        '''
        return self.connection.llen(
            name=key,
        )

    def __getstate__(self):
        '''
        '''
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
        }

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            host=value['host'],
            port=value['port'],
            database=value['database'],
        )
