from . import compressor
from . import serializer


class Encoder:
    '''
    '''
    def __init__(self, compressor_name, serializer_name):
        self.compressors = compressor.__compressors__
        self.serializers = serializer.__serializers__

        self.compressor_name = compressor_name
        self.serializer_name = serializer_name

        for compressor_obj in self.compressors:
            if compressor_obj.name == self.compressor_name:
                self.compressor = compressor_obj

                break
        else:
            raise Exception(
                'unknown compressor: {compressor_name}'.format(
                    compressor_name=compressor_name,
                )
            )

        for serializer_obj in self.serializers:
            if serializer_obj.name == self.serializer_name:
                self.serializer = serializer_obj

                break
        else:
            raise Exception(
                'unknown serializer: {serializer_name}'.format(
                    serializer_name=serializer_name,
                )
            )

    def encode(self, data):
        '''
        '''
        serialized_data = self.serializer.serialize(
            data=data,
        )
        compressed_serialized_data = self.compressor.compress(
            data=serialized_data,
        )

        return compressed_serialized_data

    def decode(self, data):
        '''
        '''
        decompressed_data = self.compressor.decompress(
            data=data,
        )
        unserialized_decompressed_data = self.serializer.unserialize(
            data=decompressed_data,
        )

        return unserialized_decompressed_data
