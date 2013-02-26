import logging
from beaker.exceptions import InvalidCacheBackendError

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager

try:
    import riak
except ImportError:
    raise InvalidCacheBackendError("Riak cache backend requires the 'riak' library")

log = logging.getLogger(__name__)

class RiakManager(NoSqlManager):
    '''
    Riak Python client packages data in JSON by default.
    '''
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port):
        self.db_conn = riak.RiakClient(host=host, port=int(port))
        self.bucket = self.db_conn.bucket('beaker_cache')

    def __contains__(self, key):
        return self.bucket.get(self._format_key(key)).exists()

    def set_value(self, key, value):
        val = self.bucket.get(self._format_key(key))
        if not val.exists():
            self.bucket.new(self._format_key(key), value).store()
        else:
            val.set_data(value)
            val.store()

    def __getitem__(self, key):
        return self.bucket.get(self._format_key(key)).get_data()

    def __delitem__(self, key):
        self.bucket.get(self._format_key(key)).delete()

    def _format_key(self, key):
        return 'beaker:%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        raise Exception("Unimplemented")

    def keys(self):
        raise Exception("Unimplemented")


class RiakContainer(Container):
    namespace_class = RiakManager
