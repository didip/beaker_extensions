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
            cluster_params['max_schema_agreement_wait'] = \
                params['max_schema_agreement_wait']

        # Clients should use any details they have to route intelligently
        if 'datacenter' in params:
            cluster_params['load_balancing_policy'] = TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=params['datacenter']))
        cluster_params['default_retry_policy'] = RetryPolicy()

        log.info(
            "CassandraCqlManager connecting to cluster with params %s",
            cluster_params)
        return Cluster(**cluster_params)

    def __resolve_hostnames(self, hostnames):
        """Resolves a list of hostnames into a list of IP addresses using DNS."""
        ips = set()
        for hostname in hostnames:
            _, _, host_ips = socket.gethostbyname_ex(hostname)
            ips.update(host_ips)
        return list(ips)

    def open_connection(self, host, port, **params):
        # NoSqlManager calls this but it's not quite the interface I want so
        # ignore it and implement a different connect method.
        pass

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
                  data blob,
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
        if self.serializer == 'json':
            encoded = json.dumps(value, ensure_ascii=True)
        else:
            encoded =  pickle.dumps(value, 2)
        if expiretime:
            ttl = int(expiretime)
            self.__session.execute(self.__set_expire_stmt,
                                   {'key': key, 'data': encoded, 'ttl': ttl})
        else:
            self.__session.execute(self.__set_no_expire_stmt,
                                   {'key': key, 'data': encoded})

    def __getitem__(self, key):
        key = self.__format_key(key)
        rows = self.__session.execute(self.__get_stmt, [key])
        if len(rows) == 0:
            return None
        assert len(rows) == 1, "get {} returned more than 1 row".format(key)
        encoded = rows[0].data
        if self.serializer == 'json':
            if isinstance(encoded, bytes):
                return json.loads(encoded.decode('utf-8'))
            else:
                return json.loads(encoded)
        else:
            return pickle.loads(encoded)

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
