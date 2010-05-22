import logging
from beaker.exceptions import InvalidCacheBackendError

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager
from beaker_extensions.nosql import pickle
 
try:
    from redis import Redis
except ImportError:
    raise InvalidCacheBackendError("PyTyrant cache backend requires the 'pytyrant' library")

log = logging.getLogger(__name__)
 
class RedisManager(NoSqlManager):
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port):
        self.db_conn = Redis(host=host, port=int(port))

    def __contains__(self, key):
        return self.db_conn.exists(self._format_key(key))

    def set_value(self, key, value):
        self.db_conn.set(key, pickle.dumps(value))

    def __delitem__(self, key):
        self.db_conn.delete(self._format_key(key))

    def _format_key(self, key):
        return 'beaker:%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        self.db_conn.flush()

    def keys(self):
        raise self.db_conn.keys('*')


class RedisContainer(Container):
    namespace_manager = RedisManager
