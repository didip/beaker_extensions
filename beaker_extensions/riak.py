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
        return self.bucket.get(key).exists()

    def set_value(self, key, value):
        self.bucket.new(key, value).store()

    def __getitem__(self, key):
        return self.bucket.get(key)

    def __delitem__(self, key):
        self.bucket.get(key).delete()

    def do_remove(self):
        raise Exception("Unimplemented")

    def keys(self):
        raise Exception("Unimplemented")


class RiakContainer(Container):
    namespace_class = RiakManager
