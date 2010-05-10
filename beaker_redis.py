import logging
from beaker_nosql import *
 
try:
  from redis import Redis
except ImportError:
  raise InvalidCacheBackendError("PyTyrant cache backend requires the 'pytyrant' library")
 
log = logging.getLogger(__name__)
 
class RedisManager(NoSqlManager):
  def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
    NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

  def open_connection(self, host, port):
    self.db_conn = Redis(host=host, port=int(port))

  def __contains__(self, key):
    return self.db_conn.exists(self._format_key(key))

  def set_value(self, key, value):
    self.db_conn.set(self._format_key(key), pickle.dumps(value))

  def __delitem__(self, key):
    self.db_conn.delete(self._format_key(key))

  def do_remove(self):
    self.db_conn.flush(all_dbs=True)

  def keys(self):
    raise self.db_conn.keys('*')
 
 
class RedisContainer(Container):
    namespace_manager = RedisManager
