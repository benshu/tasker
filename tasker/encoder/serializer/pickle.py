import pickle

from . import _serializer


class Serializer(_serializer.Serializer):
    '''
    '''
    name = 'pickle'

    @staticmethod
    def serialize(data):
        '''
        '''
        serialized_object = pickle.dumps(data)

        return serialized_object

    @staticmethod
    def unserialize(data):
        '''
        '''
        unserialized_object = pickle.loads(
            data=data,
            encoding='utf-8',
        )

        return unserialized_object
