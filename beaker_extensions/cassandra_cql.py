import json
import logging
import re

from beaker.exceptions import InvalidCacheBackendError, MissingCacheParameter

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager
from beaker_extensions.nosql import pickle

try:
    import cassandra
    from cassandra.cluster import Cluster
except ImportError:
    raise InvalidCacheBackendError(
        "cassandra_cql backend requires the 'cassandra-driver' library"
    )


log = logging.getLogger(__name__)


class CassandraCqlManager(NoSqlManager):
    """
    Cassandra backend for beaker that uses the more efficient binary protocol.

    Configuration example:
        beaker.session.type = cassandra_cql
        beaker.session.url = localhost:9160
        beaker.session.keyspace = Keyspace1
        beaker.session.column_family = beaker

    The default column_family is 'beaker'.
    If it doesn't exist under given keyspace, it is created automatically.
    """
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None,
                 keyspace=None, column_family=None, **params):
        if not keyspace:
            raise MissingCacheParameter("keyspace is required")
        if re.search(r'[^0-9a-zA-Z_]', keyspace):
            raise ValueError(
                "keyspace can only have alphanumeric chars and underscore"
            )
        self.__keyspace_cql_safe = keyspace
        table = column_family or 'beaker'
        if re.search(r'[^0-9a-zA-Z_]', table):
            raise ValueError(
                "table can only have alphanumeric chars and underscore"
            )
        self.__table_cql_safe = table
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir,
                              lock_dir=lock_dir, **params)
        if self.serializer == 'pickle':
            raise NotImplementedError(
                "Cassandra was getting angry at pickle output not being"
                "utf-8. Since we don't particularly want to be using pickle, "
                "punt on the issue and only support json for now."
            )
        assert self.serializer == 'json'

    def open_connection(self, host, port, **params):
        cluster = Cluster()
        self.__session = cluster.connect(self.__keyspace_cql_safe)
        self.__ensure_table()
        self.__prepare_statements()

    def __ensure_table(self):
        try:
            self.__session.execute('SELECT * FROM %s LIMIT 1' %
                                   self.__table_cql_safe)
        except cassandra.InvalidRequest, error:
            if 'unconfigured columnfamily' not in error.message:
                raise
            log.info("Creating new %s ColumnFamily.", self.__table_cql_safe)
            query = '''
                CREATE TABLE {tbl} (
                  key varchar,
                  data varchar,
                  PRIMARY KEY (key)
                )
            '''.format(tbl=self.__table_cql_safe)
            self.__session.execute(query)

    def __prepare_statements(self):
        contains_query = '''
            SELECT COUNT(*)
              FROM {tbl}
              WHERE key=?
        '''.format(tbl=self.__table_cql_safe)
        self.__contains_stmt = self.__session.prepare(contains_query)

        set_expire_query = '''
            INSERT INTO {tbl} (key, data)
              VALUES(?, ?)
              USING TTL ?
        '''.format(tbl=self.__table_cql_safe)
        self.__set_expire_stmt = self.__session.prepare(set_expire_query)
        set_no_expire_query = '''
            INSERT INTO {tbl} (key, data)
              VALUES(?, ?)
        '''.format(tbl=self.__table_cql_safe)
        self.__set_no_expire_stmt = self.__session.prepare(set_no_expire_query)

        get_query = '''
            SELECT data
              FROM {tbl}
              WHERE key=?
              LIMIT 2
        '''.format(tbl=self.__table_cql_safe)
        self.__get_stmt = self.__session.prepare(get_query)

        del_query = '''
            DELETE
              FROM {tbl}
              WHERE key=?
        '''.format(tbl=self.__table_cql_safe)
        self.__del_stmt = self.__session.prepare(del_query)

    def __contains__(self, key):
        rows = self.__session.execute(self.__contains_stmt,
                                      {'key': self.__format_key(key)})
        count = rows[0].count
        assert count == 0 or count == 1
        return count > 0

    def set_value(self, key, value, expiretime=None):
        key = self.__format_key(key)
        data = json.dumps(value)
        if expiretime:
            ttl = int(expiretime)
            self.__session.execute(self.__set_expire_stmt,
                                   {'key': key, 'data': data, 'ttl': ttl})
        else:
            self.__session.execute(self.__set_no_expire_stmt,
                                   {'key': key, 'data': data})

    def __getitem__(self, key):
        key = self.__format_key(key)
        rows = self.__session.execute(self.__get_stmt, [key])
        if len(rows) == 0:
            return None
        assert len(rows) == 1, "get {} returned more than 1 row".format(key)
        encoded = rows[0].data
        if isinstance(encoded, bytes):
            return json.loads(encoded.decode('utf-8'))
        else:
            return json.loads(encoded)

    def __delitem__(self, key):
        key = self.__format_key(key)
        res = self.__session.execute(self.__del_stmt, [key])

    def __format_key(self, key):
        return '%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        """DELETE EVERYTHING!"""
        self.__session.execute('TRUNCATE ' + self.__table_cql_safe)

    def keys(self):
        raise NotImplementedError(
            "keys() is weird. NoSqlManager's version seems to return the db's "
            "keys, not the keys used by the rest of this class' api. "
            "Regardless, it doesn't seem to be used so I'm not implementing it."
        )


class CassandraCqlContainer(Container):
    namespace_class = CassandraCqlManager
