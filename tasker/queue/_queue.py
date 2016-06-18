from .. import logger


class Queue:
    '''
    '''
    name = 'Queue'

    def __init__(self, queue_name, connector, encoder):
        '''
        '''
        self.logger = logger.logger.Logger(
            logger_name=self.name,
        )

        self.queue_name = queue_name

        self.connector = connector
        self.encoder = encoder

    def dequeue(self, timeout=0):
        '''
        '''
        try:
            value = self._dequeue(
                timeout=timeout,
            )
            if not value:
                return {}

            decoded_value = self.encoder.decode(
                data=value,
            )

            return decoded_value
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _dequeue(self, timeout):
        '''
        '''
        raise NotImplemented()

    def dequeue_bulk(self, count):
        '''
        '''
        try:
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
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _dequeue_bulk(self, count):
        '''
        '''
        raise NotImplemented()

    def enqueue(self, value):
        '''
        '''
        try:
            encoded_value = self.encoder.encode(
                data=value,
            )

            self._enqueue(
                value=encoded_value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _enqueue(self, timeout):
        '''
        '''
        raise NotImplemented()

    def enqueue_bulk(self, values):
        '''
        '''
        try:
            encoded_values = []

            for value in values:
                encoded_value = self.encoder.encode(
                    data=value,
                )

                encoded_values.append(encoded_value)

            self._enqueue_bulk(
                values=encoded_values,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _enqueue_bulk(self, count):
        '''
        '''
        raise NotImplemented()

    def add_result(self, value):
        '''
        '''
        try:
            return self._add_result(
                value=value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _add_result(self, value):
        '''
        '''
        raise NotImplemented()

    def remove_result(self, value):
        '''
        '''
        try:
            return self._remove_result(
                value=value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _remove_result(self, value):
        '''
        '''
        raise NotImplemented()

    def has_result(self, value):
        '''
        '''
        try:
            return self._has_result(
                value=value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _has_result(self, value):
        '''
        '''
        raise NotImplemented()

    def len(self):
        '''
        '''
        try:
            return self._len()
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _len(self):
        '''
        '''
        raise NotImplemented()

    def flush(self):
        '''
        '''
        try:
            return self._flush()
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _flush(self):
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

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            queue_name=value['queue_name'],
            connector=value['connector'],
            encoder=value['encoder'],
        )
