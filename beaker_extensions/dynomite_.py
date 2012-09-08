import logging
from beaker.exceptions import InvalidCacheBackendError

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager

try:
    from dynomite import Dynomite
    from dynomite.ttypes import *
except ImportError:
    raise InvalidCacheBackendError("Dynomite cache backend requires the 'dynomite' library")

try:
    from thrift import Thrift
    from thrift.transport import TSocket
    from thrift.transport import TTransport
    from thrift.protocol import TBinaryProtocol
except ImportError:
    raise InvalidCacheBackendError("Dynomite cache backend requires the 'thrift' library")

log = logging.getLogger(__name__)

class DynomiteManager(NoSqlManager):
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port):
        self.transport = TSocket.TSocket(host, int(port))
        self.transport = TTransport.TBufferedTransport(transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.db_conn = Dynomite.Client(protocol)
        self.transport.open()

    def __contains__(self, key):
        return self.db_conn.has(self._format_key(key))

    def has_key(self, key):
        return key in self

    def set_value(self, key, value):
        self.db_conn.put(self._format_key(key), None, value)

    def __delitem__(self, key):
        self.db_conn.remove(self._format_key(key))

    def do_remove(self):
        raise Exception("Unimplemented")

    def keys(self):
        raise Exception("Unimplemented")


class DynomiteContainer(Container):
    namespace_class = DynomiteManager
