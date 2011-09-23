import logging
from beaker.exceptions import InvalidCacheBackendError

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager
from beaker_extensions.nosql import pickle

try:
    import couchdb
    from couchdb.mapping import Document
    from couchdb.http import ResourceNotFound, ResourceConflict
except ImportError:
    raise InvalidCacheBackendError("CouchDB cache backend requires the 'couchdb-python' library")

log = logging.getLogger(__name__)

class CouchDBManager(NoSqlManager):
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
        self.database = params['database']
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port, **params):
        self.db_conn = couchdb.Server('http://'+host+':'+str(port)+'/')[self.database]

    def __contains__(self, key):
        key = self._format_key(key)
        try:
            doc = self.db_conn[key]
        except ResourceNotFound:
            return False
        return True

    def __getitem__(self, key):
        try:
            return pickle.loads(self.db_conn[self._format_key(key)]['value'])
        except ResourceNotFound:
            return None

    def set_value(self, key, value):
        key = self._format_key(key)
        doc = {}
        try:
            doc = self.db_conn[key]
        except ResourceNotFound:
            doc['_id'] = key
            doc['type'] = 'beaker.session'
        doc['value'] = pickle.dumps(value)
        while True:
            try:
                self.db_conn.save(doc)
                break
            except ResourceConflict:
                doc = self.db_conn[key]
                doc['value'] = pickle.dumps(value)

    def __delitem__(self, key):
        key = self._format_key(key)
        doc = self.db_conn[key]
        self.db_conn.delete(doc)

    def _format_key(self, key):
        return 'beaker:%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        map_fun = '''function(doc) {
            if (doc.type == 'beaker.session')
                emit(doc.id, null);
        }'''
        for row in self.db_conn.query(map_fun):
            doc = self.db_conn.get(row.id)
            self.db_conn.delete(doc)

    def keys(self):
        raise Exception("Unimplemented")


class CouchDBContainer(Container):
    namespace_manager = CouchDBManager
