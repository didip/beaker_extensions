import logging
from beaker.exceptions import InvalidCacheBackendError, MissingCacheParameter

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager
from beaker_extensions.nosql import pickle

try:
    import pycassa
except ImportError:
    raise InvalidCacheBackendError("Cassandra cache backend requires the 'pycassa' library")

log = logging.getLogger(__name__)

class CassandraManager(NoSqlManager):
    """
    Cassandra backend for beaker.

    Configuration example:
        beaker.session.type = cassandra
        beaker.session.url = localhost:9160
        beaker.session.keyspace = Keyspace1
        beaker.session.column_family = beaker

    The default column_family is 'beaker'.
    If it doesn't exist under given keyspace, it is created automatically.
    """
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, keyspace=None, column_family=None, **params):
        if not keyspace:
            raise MissingCacheParameter("keyspace is required")
        self.keyspace = keyspace
        self.column_family = column_family or 'beaker'
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port, **params):
        self.pool = pycassa.ConnectionPool(self.keyspace)
        try:
            self.cf = pycassa.ColumnFamily(self.pool, self.column_family)
        except pycassa.NotFoundException:
            log.info("Creating new %s ColumnFamily." % self.column_family)
            system_manager = pycassa.system_manager.SystemManager()
            system_manager.create_column_family(self.keyspace, self.column_family)
            self.cf = pycassa.ColumnFamily(self.pool, self.column_family)

    def __contains__(self, key):        
        return self.cf.get_count(self._format_key(key)) > 0

    def set_value(self, key, value, expiretime=None):
        key = self._format_key(key)
        self.cf.insert(key, {'data': pickle.dumps(value)}, ttl=expiretime)

    def __getitem__(self, key):
        try:
            result = self.cf.get(self._format_key(key))
            return pickle.loads(result['data'])
        except pycassa.NotFoundException:
            return None

    def __delitem__(self, key):        
        key = self._format_key(key)
        self.cf.remove(self._format_key(key))

    def _format_key(self, key):
        return '%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        for key, empty in cf.get_range(column_count=0, filter_empty=False):
            cf.remove(key)

    def keys(self):
        return list(key for key, empty in self.cf.get_range(column_count=0, filter_empty=False))


class CassandraContainer(Container):
    namespace_class = CassandraManager
