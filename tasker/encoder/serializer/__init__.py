from . import msgpack
from . import pickle

from . import _serializer


__serializers__ = [
    msgpack.Serializer,
    pickle.Serializer,
]
