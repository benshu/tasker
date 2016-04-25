import pickle
import msgpack
import zlib
import gzip
import bz2
import lzma
import logging


class Queue:
    '''
    '''
    log_level = logging.INFO

    def __init__(self, connector, queue_name, serializer, compressor):
        '''
        '''
        self.logger = self._create_logger()

        self.connector = connector
        self.queue_name = queue_name
        self.serializer = serializer
        self.compressor = compressor

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

    def dequeue_bulk(self, count):
        '''
        '''
        values = self.connector.pop_bulk(
            key=self.queue_name,
            count=count,
        )

        self.logger.debug('popped bulk')

        decoded_values = []
        for value in values:
            decoded_value = self._decode(
                value=value,
            )
            decoded_values.append(decoded_value)

        self.logger.debug('decoded bulk')

        return decoded_values

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

    def enqueue_bulk(self, values):
        '''
        '''
        encoded_values = []
        for value in values:
            encoded_value = self._encode(
                value=value,
            )
            encoded_values.append(encoded_value)

        self.logger.debug('encoded bulk')

        pushed = self.connector.push_bulk(
            key=self.queue_name,
            values=encoded_values,
        )

        self.logger.debug('pushed bulk')

        return pushed

    def _compress(self, value):
        '''
        '''
        if self.compressor == 'zlib':
            compressed_object = zlib.compress(value)

            self.logger.debug('zlib compressed')
        elif self.compressor == 'gzip':
            compressed_object = gzip.compress(value)

            self.logger.debug('gzip compressed')
        elif self.compressor == 'bzip2':
            compressed_object = bz2.compress(value)

            self.logger.debug('bzip2 compressed')
        elif self.compressor == 'lzma':
            compressed_object = lzma.compress(value)

            self.logger.debug('lzma compressed')
        else:
            compressed_object = value

            self.logger.debug('did not compress')

        return compressed_object

    def _decompress(self, value):
        '''
        '''
        if self.compressor == 'zlib':
            decompressed_object = zlib.decompress(value)

            self.logger.debug('zlib decompressed')
        elif self.compressor == 'gzip':
            decompressed_object = gzip.decompress(value)

            self.logger.debug('gzip decompressed')
        elif self.compressor == 'bzip2':
            decompressed_object = bz2.decompress(value)

            self.logger.debug('bzip2 decompressed')
        elif self.compressor == 'lzma':
            decompressed_object = lzma.decompress(value)

            self.logger.debug('lzma decompressed')
        else:
            decompressed_object = value

            self.logger.debug('did not decompress')

        return decompressed_object

    def _serialize(self, value):
        '''
        '''
        if self.serializer == 'pickle':
            serialized_value = pickle.dumps(value)
        elif self.serializer == 'msgpack':
            serialized_value = msgpack.dumps(value)
        else:
            serialized_value = value

        return serialized_value

    def _unserialize(self, value):
        '''
        '''
        if self.serializer == 'pickle':
            unserialized_value = pickle.loads(
                data=value,
                encoding='utf-8',
            )
        elif self.serializer == 'msgpack':
            unserialized_value = msgpack.loads(
                packed=value,
                encoding='utf-8',
            )
        else:
            unserialized_value = value

        return unserialized_value

    def _encode(self, value):
        '''
        '''
        serialized_value = self._serialize(
            value=value,
        )
        compressed_serialized_value = self._compress(
            value=serialized_value,
        )

        self.logger.debug('encoded')

        return compressed_serialized_value

    def _decode(self, value):
        '''
        '''
        decompressed_value = self._decompress(
            value=value,
        )
        unserialized_decompressed_value = self._unserialize(
            value=decompressed_value,
        )

        self.logger.debug('decoded')

        return unserialized_decompressed_value

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
            'compressor': self.compressor,
            'serializer': self.serializer,
        }

        self.logger.debug('getstate')

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            connector=value['connector'],
            queue_name=value['queue_name'],
            compressor=value['compressor'],
            serializer=value['serializer'],
        )

        self.logger.debug('setstate')
