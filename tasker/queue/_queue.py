from .. import logger


class Queue:
    '''
    '''
    name = 'Queue'

    def __init__(self, connector, encoder):
        '''
        '''
        self.logger = logger.logger.Logger(
            logger_name=self.name,
        )

        self.connector = connector
        self.encoder = encoder

    def dequeue(self, queue_name):
        '''
        '''
        try:
            value = self._dequeue(
                queue_name=queue_name,
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

    def _dequeue(self, queue_name, timeout):
        '''
        '''
        raise NotImplemented()

    def dequeue_bulk(self, queue_name, count):
        '''
        '''
        try:
            decoded_values = []

            values = self._dequeue_bulk(
                queue_name=queue_name,
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

    def _dequeue_bulk(self, queue_name, count):
        '''
        '''
        raise NotImplemented()

    def enqueue(self, queue_name, value):
        '''
        '''
        try:
            encoded_value = self.encoder.encode(
                data=value,
            )

            self._enqueue(
                queue_name=queue_name,
                value=encoded_value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _enqueue(self, queue_name, timeout):
        '''
        '''
        raise NotImplemented()

    def enqueue_bulk(self, queue_name, values):
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
                queue_name=queue_name,
                values=encoded_values,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _enqueue_bulk(self, queue_name, count):
        '''
        '''
        raise NotImplemented()

    def add_result(self, queue_name, value):
        '''
        '''
        try:
            return self._add_result(
                queue_name=queue_name,
                value=value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _add_result(self, queue_name, value):
        '''
        '''
        raise NotImplemented()

    def remove_result(self, queue_name, value):
        '''
        '''
        try:
            return self._remove_result(
                queue_name=queue_name,
                value=value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _remove_result(self, queue_name, value):
        '''
        '''
        raise NotImplemented()

    def has_result(self, queue_name, value):
        '''
        '''
        try:
            return self._has_result(
                queue_name=queue_name,
                value=value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _has_result(self, queue_name, value):
        '''
        '''
        raise NotImplemented()

    def len(self, queue_name):
        '''
        '''
        try:
            return self._len(
                queue_name=queue_name,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _len(self, queue_name):
        '''
        '''
        raise NotImplemented()

    def flush(self, queue_name):
        '''
        '''
        try:
            return self._flush(
                queue_name=queue_name,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _flush(self, queue_name):
        '''
        '''
        raise NotImplemented()

    def __getstate__(self):
        '''
        '''
        state = {
            'connector': self.connector,
            'encoder': self.encoder,
        }

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            connector=value['connector'],
            encoder=value['encoder'],
        )
