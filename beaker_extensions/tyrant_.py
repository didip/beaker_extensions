# Courtesy of: http://www.jackhsu.com/2009/05/27/pylons-with-tokyo-cabinet-beaker-sessions
import logging
from beaker.exceptions import InvalidCacheBackendError

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager
from beaker_extensions.nosql import pickle

try:
    from pytyrant import PyTyrant
except ImportError:
    raise InvalidCacheBackendError("PyTyrant cache backend requires the 'pytyrant' library")

log = logging.getLogger(__name__)

class TokyoTyrantManager(NoSqlManager):
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port):
        self.db_conn = PyTyrant.open(host, int(port))

    def __contains__(self, key):
        return self.db_conn.has_key(self._format_key(key))

    def set_value(self, key, value):
        self.db_conn[self._format_key(key)] =  pickle.dumps(value)

    def __delitem__(self, key):
        del self.db_conn[self._format_key(key)]

    def do_remove(self):
        self.db_conn.clear()

    def keys(self):
        return self.db_conn.keys()


class TokyoTyrantContainer(Container):
    namespace_class = TokyoTyrantManager
