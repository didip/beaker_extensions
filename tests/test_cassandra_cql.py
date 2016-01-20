import unittest

from nose.plugins.attrib import attr
from nose.tools import nottest

from beaker.cache import Cache

from common import CommonMethodMixin


class CassandraCqlSetup(object):
    __keyspace = 'test_ks'
    __table = 'test_table'

    @classmethod
    def setUpClass(cls):
        import cassandra
        from cassandra.cluster import Cluster
        cluster = Cluster()
        cls.__session = cluster.connect()
        try:
            cls.__create_keyspace()
        except cassandra.AlreadyExists:
            cls.__delete_keyspace()
            cls.__create_keyspace()

    @classmethod
    def teardownAll(cls):
        cls.__delete_keyspace()

    @classmethod
    def __create_keyspace(cls):
        query = '''
            CREATE KEYSPACE %s
              WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        ''' % cls.__keyspace
        cls.__session.execute(query)

    @classmethod
    def __delete_keyspace(cls):
        query = 'DROP KEYSPACE %s' % cls.__keyspace
        cls.__session.execute(query)

    def setUp(self):
        raise NotImplementedError


class CassandraCqlPickleSetup(object):
    __keyspace = 'test_ks'
    __table = 'test_table'

    def setUp(self):
        self.cache = Cache('testns', type='cassandra_cql',
                           url='localhost:9042', keyspace=self.__keyspace,
                           column_family=self.__table, serializer='pickle')
        self.cache.clear()

class CassandraCqlJsonSetup(object):
    __keyspace = 'test_ks'
    __table = 'test_table'

    def setUp(self):
        self.cache = Cache('testns', type='cassandra_cql',
                           url='localhost:9042', keyspace=self.__keyspace,
                           column_family=self.__table, serializer='json')
        self.cache.clear()


class CassandraTestOverrides(object):
    @nottest
    def test_fresh_createfunc(self):
        # createfunc depends on create_lock being implemented which it isn't for
        # casandra_cql, so skip this test which would otherwise run from
        # CommonMethodMixin.
        pass

    @nottest
    def test_multi_keys(self):
        # This also uses createfunc which it isn't for casandra_cql, so skip
        # this test which would otherwise run from CommonMethodMixin.
        pass

    def test_invalid_keyspace(self):
        try:
            Cache('testns', type='cassandra_cql', url='localhost:9042',
                  keyspace='no spaces allowed', column_family='whatever',
                  serializer='json')
        except ValueError, error:
            if 'keyspace can only have' not in error.message:
                raise

    def test_invalid_table(self):
        try:
            Cache('testns', type='cassandra_cql', url='localhost:9042',
                  keyspace='whatever', column_family='no spaces allowed',
                  serializer='json')
        except ValueError, error:
            if 'table can only have' not in error.message:
                raise


@attr('cassandra_cql')
class TestCassandraCqlPickle(CassandraCqlPickleSetup, CassandraCqlSetup,
                             CassandraTestOverrides, CommonMethodMixin,
                             unittest.TestCase):
    pass


@attr('cassandra_cql')
class TestCassandraCqlJson(CassandraCqlJsonSetup, CassandraCqlSetup,
                           CassandraTestOverrides, CommonMethodMixin,
                           unittest.TestCase):
    @nottest
    def test_store_obj(self):
        # We can't store objects with the json serializer so skip this test from
        # the CommonMethodMixin.
        pass
