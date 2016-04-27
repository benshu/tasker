import logging

from . import _queue


class Queue(_queue.Queue):
    '''
    '''
    name = 'regular'

    log_level = logging.INFO

    def dequeue(self, timeout=0):
        '''
        '''
        value = self.connector.pop(
            key=self.queue_name,
            timeout=timeout,
        )

        if value is None:
            return None

        self.logger.debug('popped')

        return value

    def dequeue_bulk(self, count):
        '''
        '''
        values = self.connector.pop_bulk(
            key=self.queue_name,
            count=count,
        )

        self.logger.debug('popped bulk')

        return values

    def enqueue(self, value):
        '''
        '''
        pushed = self.connector.push(
            key=self.queue_name,
            value=value,
        )

        self.logger.debug('pushed')

        return pushed

    def enqueue_bulk(self, values):
        '''
        '''
        pushed = self.connector.push_bulk(
            key=self.queue_name,
            values=values,
        )

        self.logger.debug('pushed bulk')

        return pushed

    def len(self):
        '''
        '''
        queue_len = self.connector.len(
            key=self.queue_name,
        )

        self.logger.debug('len')

        return queue_len

    def flush(self):
        '''
        '''
        self.connector.delete(
            key=self.queue_name,
        )

        self.logger.debug('flushed')
