from . import redis
from . import redis_cluster

from . import _connector


__connectors__ = {
    redis.Connector.name: redis.Connector,
    redis_cluster.Connector.name: redis_cluster.Connector,
}
