import logging
from beaker.exceptions import InvalidCacheBackendError

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager
from beaker_extensions.nosql import pickle

try:
    from redis import StrictRedis, ConnectionPool
except ImportError:
    raise InvalidCacheBackendError("Redis cache backend requires the 'redis' library")

log = logging.getLogger(__name__)

class RedisManager(NoSqlManager):
    def __init__(self,
                 namespace,
                 url=None,
                 data_dir=None,
                 lock_dir=None,
                 **params):
        self.db = params.pop('db', None)
        self.connection_pools = {}
        NoSqlManager.__init__(self,
                              namespace,
                              url=url,
                              data_dir=data_dir,
                              lock_dir=lock_dir,
                              **params)

    def open_connection(self, host, port, **params):
        pool_key = self._format_pool_key(host, port, self.db)
        if pool_key not in self.connection_pools:
            self.connection_pools[pool_key] = ConnectionPool(host=host,
                                                             port=port,
                                                             db=self.db)
        self.db_conn = StrictRedis(connection_pool=self.connection_pools[pool_key],
                                   **params)

    def __contains__(self, key):
        return self.db_conn.exists(self._format_key(key))

    def set_value(self, key, value, expiretime=None):
        key = self._format_key(key)

        #
        # beaker.container.Value.set_value calls NamespaceManager.set_value
        # however it (until version 1.6.4) never sets expiretime param.
        #
        # Checking "type(value) is tuple" is a compromise
        # because Manager class can be instantiated outside container.py (See: session.py)
        #
        if (expiretime is None) and (type(value) is tuple):
            expiretime = value[1]

        if expiretime:
            self.db_conn.setex(key, expiretime, pickle.dumps(value, 2))
        else:
            self.db_conn.set(key, pickle.dumps(value, 2))

    def __delitem__(self, key):
        self.db_conn.delete(self._format_key(key))

    def _format_key(self, key):
        return 'beaker:%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def _format_pool_key(self, host, port, db):
        return '{0}:{1}:{2}'.format(host, port, self.db)

    def do_remove(self):
        self.db_conn.flush()

    def keys(self):
        return self.db_conn.keys('beaker:%s:*' % self.namespace)


class RedisContainer(Container):
    namespace_class = RedisManager
