import logging


class Queue:
    '''
    '''
    name = ''

    log_level = logging.INFO

    def __init__(self, queue_name, connector):
        '''
        '''
        self.queue_name = queue_name
        self.connector = connector

        self.logger = self._create_logger()

        self.logger.debug('initialized')

    def _create_logger(self):
        '''
        '''
        logger = logging.getLogger(
            name='Queue',
        )

        for handler in logger.handlers:
            logger.removeHandler(handler)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt='%(asctime)s %(name)-12s %(levelname)-8s %(funcName)-16s -> %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.setLevel(self.log_level)

        return logger

    def dequeue(self, timeout=0):
        '''
        '''
        raise NotImplemented()

    def dequeue_bulk(self, count):
        '''
        '''
        raise NotImplemented()

    def enqueue(self, value):
        '''
        '''
        raise NotImplemented()

    def enqueue_bulk(self, values):
        '''
        '''
        raise NotImplemented()

    def len(self):
        '''
        '''
        raise NotImplemented()

    def flush(self):
        '''
        '''
        raise NotImplemented()

    def __getstate__(self):
        '''
        '''
        state = {
            'queue_name': self.queue_name,
            'connector': self.connector,
        }

        self.logger.debug('getstate')

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            queue_name=value['queue_name'],
            connector=value['connector'],
        )

        self.logger.debug('setstate')
