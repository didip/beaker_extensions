from datetime import datetime
import json
import logging
import random
import re
import socket

from beaker.exceptions import InvalidCacheBackendError, MissingCacheParameter

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager
from beaker_extensions.nosql import pickle

try:
    import cassandra
    from cassandra.cluster import Cluster
    from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy, RetryPolicy
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
        beaker.session.url = cassandra1:9042;cassandra2:9042
        beaker.session.keyspace = Keyspace1
        beaker.session.column_family = beaker

    The default column_family is 'beaker'.
    If it doesn't exist under given keyspace, it is created automatically.
    """

    connection_pools = {}

    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None,
                 keyspace=None, column_family=None, **params):
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir,
                              lock_dir=lock_dir, **params)
        connection_key = '-'.join([
            '%r' % url,
            '%r' % keyspace,
            '%r' % column_family,
        ] + ['%s:%r' % (k, params[k]) for k in params])
        if connection_key in self.connection_pools:
            self.db_conn = self.connection_pools[connection_key]
        else:
            self.db_conn = _CassandraBackedDict(namespace, url, keyspace,
                                                column_family, **params)
            self.connection_pools[connection_key] = self.db_conn

    def open_connection(self, host, port, **params):
        # NoSqlManager calls this but it's not quite the interface I want so
        # ignore it.
        pass

    def __setitem__(self, key, value):
        # NoSqlManager.__setitem__() passes expiretime as an arg to set_value.
        # This is wrong because its set_value() doesn't take it. Rather than
        # mess with it, override __setitem__ with a version that doesn't pass
        # expiretime. We'll use the _CassandraBackedDict's field directly.
        self.set_value(key, value)

    def _format_key(self, key):
        return '%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def get_creation_lock(self, key):
        raise NotImplementedError()


class _CassandraBackedDict(object):
    """
    A class with an interface very similar to a dict that NoSqlManager will use.
    """

    def __init__(self, namespace, url=None, keyspace=None, column_family=None,
                 **params):
        if not keyspace:
            raise MissingCacheParameter("keyspace is required")
        if re.search(r'\W', keyspace):
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
        expire = params.get('expire', None)
        self._expiretime = int(expire) if expire else None

        cluster = self.__connect_to_cluster(url, params)
        self.__session = cluster.connect(self.__keyspace_cql_safe)
        self.__ensure_table()
        self.__prepare_statements()

    def __connect_to_cluster(self, urls, params):
        cluster_params = {}

        # If the config specifies a hostname which resolves to multiple
        # hosts (eg dns roundrobin or consul), the driver will have a
        # shorter timeout than we want since the timeout is applied per item
        # in 'contact_points'. To avoid this, resolve the the host
        # explicitly and pass in up to 2 random ones.
        url_list = [h.strip() for h in urls.split(';')]
        hosts = [h.split(':', 1)[0] for h in url_list]
        contact_points = self.__resolve_hostnames(hosts)
        random.shuffle(contact_points)
        cluster_params['contact_points'] = contact_points[:2]

        if 'max_schema_agreement_wait' in params:
            cluster_params['max_schema_agreement_wait'] = int(
                params['max_schema_agreement_wait'])

        # Clients should use any details they have to route intelligently
        if 'datacenter' in params:
            cluster_params['load_balancing_policy'] = TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=params['datacenter']))
        cluster_params['default_retry_policy'] = RetryPolicy()

        log.info(
            "Connecting to cassandra cluster with params %s",
            cluster_params)
        return Cluster(**cluster_params)

    def __resolve_hostnames(self, hostnames):
        """Resolves a list of hostnames into a list of IP addresses using DNS."""
        ips = set()
        for hostname in hostnames:
            _, _, host_ips = socket.gethostbyname_ex(hostname)
            ips.update(host_ips)
        return list(ips)

    def __ensure_table(self):
        try:
            self.__session.execute('SELECT * FROM %s LIMIT 1' %
                                   self.__table_cql_safe)
        except cassandra.InvalidRequest, error:
            if not ('unconfigured columnfamily' in error.message or
                    'unconfigured table' in error.message):
                raise
            log.info("Creating new %s ColumnFamily.", self.__table_cql_safe)
            query = '''
                CREATE TABLE {tbl} (
                  key varchar,
                  data blob,
                  updated_at timestamp,
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
            INSERT INTO {tbl} (key, data, updated_at)
              VALUES(?, ?, ?)
              USING TTL ?
        '''.format(tbl=self.__table_cql_safe)
        self.__set_expire_stmt = self.__session.prepare(set_expire_query)
        set_no_expire_query = '''
            INSERT INTO {tbl} (key, data, updated_at)
              VALUES(?, ?, ?)
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

    def has_key(self, key):
        # NoSqlManager uses has_key() rather than `in`.
        rows = self.__session.execute(self.__contains_stmt, {'key': key})
        count = rows[0].count
        assert count == 0 or count == 1
        return count > 0

    def __setitem__(self, key, value):
        if self._expiretime:
            self.__session.execute(self.__set_expire_stmt,
                                   {'key': key, 'data': value,
                                    'ttl': self._expiretime,
                                    'updated_at': datetime.utcnow()})
        else:
            self.__session.execute(self.__set_no_expire_stmt,
                                   {'key': key, 'data': value,
                                    'updated_at': datetime.utcnow()})

    def get(self, key):
        # NoSqlManager uses get() rather than [].
        #
        # The caller has to make an iterator out of a ResultSet:
        # https://datastax-oss.atlassian.net/browse/PYTHON-463
        rows = iter(self.__session.execute(self.__get_stmt, [key]))
        try:
            res = rows.next()
        except StopIteration:
            return None
        try:
            rows.next()
            assert False, "get {} found more than 1 row".format(key)
        except StopIteration:
            pass
        return res.data

    def __delitem__(self, key):
        res = self.__session.execute(self.__del_stmt, [key])

    def clear(self):
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
