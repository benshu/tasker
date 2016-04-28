from . import msgpack
from . import pickle

from . import _serializer


__serializers__ = {
    msgpack.Serializer.name: msgpack.Serializer,
    pickle.Serializer.name: pickle.Serializer,
}
