import pickle
import gzip
import bz2
import lzma
import logging


class Queue:
    '''
    '''
    log_level = logging.INFO

    def __init__(self, connector, queue_name, compression):
        '''
        '''
        self.logger = self._create_logger()

        self.connector = connector
        self.queue_name = queue_name
        self.compression = compression

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
        value = self.connector.pop(
            key=self.queue_name,
            timeout=timeout,
        )

        if value is None:
            return None

        self.logger.debug('popped')

        decoded_value = self._decode(
            value=value,
        )

        self.logger.debug('decoded')

        return decoded_value

    def enqueue(self, value):
        '''
        '''
        encoded_value = self._encode(
            value=value,
        )

        self.logger.debug('encoded')

        pushed = self.connector.push(
            key=self.queue_name,
            value=encoded_value,
        )

        self.logger.debug('pushed')

        return pushed

    def _encode(self, value):
        '''
        '''
        pickled_value = pickle.dumps(value)

        self.logger.debug('pickled')

        if self.compression == 'gzip':
            compressed_pickled_value = gzip.compress(
                data=pickled_value,
            )

            self.logger.debug('gzip compressed')
        elif self.compression == 'bzip2':
            compressed_pickled_value = bz2.compress(
                data=pickled_value,
            )

            self.logger.debug('bzip2 compressed')
        elif self.compression == 'lzma':
            compressed_pickled_value = lzma.compress(
                data=pickled_value,
            )

            self.logger.debug('lzma compressed')
        else:
            compressed_pickled_value = pickled_value

            self.logger.debug('did not compress')

        return compressed_pickled_value

    def _decode(self, value):
        '''
        '''
        if self.compression == 'gzip':
            decompressed_value = gzip.decompress(
                data=value,
            )

            self.logger.debug('gzip decompressed')
        elif self.compression == 'bzip2':
            decompressed_value = bz2.decompress(
                data=value,
            )

            self.logger.debug('bzip2 decompressed')
        elif self.compression == 'lzma':
            decompressed_value = lzma.decompress(
                data=value,
            )

            self.logger.debug('lzma decompressed')
        else:
            decompressed_value = value

            self.logger.debug('did not decompress')

        decompressed_unpickled_value = pickle.loads(decompressed_value)

        self.logger.debug('unpickled')

        return decompressed_unpickled_value

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

    def __getstate__(self):
        '''
        '''
        state = {
            'connector': self.connector,
            'queue_name': self.queue_name,
            'compression': self.compression,
        }

        self.logger.debug('getstate')

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            connector=value['connector'],
            queue_name=value['queue_name'],
            compression=value['compression'],
        )

        self.logger.debug('setstate')
