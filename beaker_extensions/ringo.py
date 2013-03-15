import logging
from beaker.exceptions import InvalidCacheBackendError

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager
from beaker_extensions.nosql import pickle

try:
    from ringogw import Ringo
except ImportError:
    raise InvalidCacheBackendError("Ringo cache backend requires the 'ringogw' library")

log = logging.getLogger(__name__)

class RingoManager(NoSqlManager):
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port):
        self.domain = 'default'
        self.db_conn = Ringo("%s:%s" % (host, port))

    def __contains__(self, key):
        raise Exception("Unimplemented")

    def __getitem__(self, key):
        return pickle.loads(self.db_conn.get(self.domain, self._format_key(key)))

    def set_value(self, key, value):
        self.db_conn.put(self.domain, self._format_key(key), pickle.dumps(value, 2))

    def __delitem__(self, key):
        raise Exception("Unimplemented")

    def do_remove(self):
        raise Exception("Unimplemented")

    def keys(self):
        raise Exception("Unimplemented")


class RingoContainer(Container):
    namespace_class = RingoManager
