import logging


class Queue:
    '''
    '''
    name = ''

    log_level = logging.INFO

    def __init__(self, queue_name, connector, encoder):
        '''
        '''
        self.queue_name = queue_name
        self.connector = connector
        self.encoder = encoder

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
        value = self._dequeue(
            timeout=timeout,
        )

        decoded_value = self.encoder.decode(
            data=value,
        )

        return decoded_value

    def _dequeue(self, timeout):
        '''
        '''
        raise NotImplemented()

    def dequeue_bulk(self, count):
        '''
        '''
        decoded_values = []

        values = self._dequeue_bulk(
            count=count,
        )

        for value in values:
            decoded_value = self.encoder.decode(
                data=value,
            )

            decoded_values.append(decoded_value)

        return decoded_values

    def _dequeue_bulk(self, count):
        '''
        '''
        raise NotImplemented()

    def enqueue(self, value):
        '''
        '''
        encoded_value = self.encoder.encode(
            data=value,
        )

        self._enqueue(
            value=encoded_value,
        )

    def _enqueue(self, timeout):
        '''
        '''
        raise NotImplemented()

    def enqueue_bulk(self, values):
        '''
        '''
        encoded_values = []

        for value in values:
            encoded_value = self.encoder.encode(
                data=value,
            )

            encoded_values.append(encoded_value)

        self._enqueue_bulk(
            values=encoded_values,
        )

    def _enqueue_bulk(self, count):
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
            'encoder': self.encoder,
        }

        self.logger.debug('getstate')

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            queue_name=value['queue_name'],
            connector=value['connector'],
            encoder=value['encoder'],
        )

        self.logger.debug('setstate')
