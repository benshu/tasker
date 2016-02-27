import pickle
import gzip
import bz2
import lzma


class Queue:
    '''
    '''
    def __init__(self, connector, queue_name, compression):
        '''
        '''
        self.connector = connector
        self.queue_name = queue_name
        self.compression = compression

    def dequeue(self, timeout=0):
        '''
        '''
        value = self.connector.pop(
            key=self.queue_name,
            timeout=timeout,
        )

        if value is None:
            return None

        decoded_value = self._decode(
            value=value,
        )

        return decoded_value

    def enqueue(self, value):
        '''
        '''
        encoded_value = self._encode(
            value=value,
        )

        return self.connector.push(
            key=self.queue_name,
            value=encoded_value,
        )

    def _encode(self, value):
        '''
        '''
        pickled_value = pickle.dumps(value)

        if self.compression == 'gzip':
            compressed_pickled_value = gzip.compress(
                data=pickled_value,
            )
        elif self.compression == 'bzip2':
            compressed_pickled_value = bz2.compress(
                data=pickled_value,
            )
        elif self.compression == 'lzma':
            compressed_pickled_value = lzma.compress(
                data=pickled_value,
            )
        else:
            compressed_pickled_value = pickled_value

        return compressed_pickled_value

    def _decode(self, value):
        '''
        '''
        if self.compression == 'gzip':
            decompressed_value = gzip.decompress(
                data=value,
            )
        elif self.compression == 'bzip2':
            decompressed_value = bz2.decompress(
                data=value,
            )
        elif self.compression == 'lzma':
            decompressed_value = lzma.decompress(
                data=value,
            )
        else:
            decompressed_value = value

        decompressed_unpickled_value = pickle.loads(decompressed_value)

        return decompressed_unpickled_value

    def len(self):
        '''
        '''
        return self.connector.len(
            key=self.queue_name,
        )

    def flush(self):
        '''
        '''
        self.connector.delete(
            key=self.queue_name,
        )

    def __getstate__(self):
        '''
        '''
        return {
            'connector': self.connector,
            'queue_name': self.queue_name,
            'compression': self.compression,
        }

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            connector=value['connector'],
            queue_name=value['queue_name'],
            compression=value['compression'],
        )
