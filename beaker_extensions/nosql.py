import json
import logging

from beaker.container import NamespaceManager, Container
from beaker.synchronization import file_synchronizer
from beaker.util import verify_directory
from beaker.exceptions import MissingCacheParameter

try:
    import cPickle as pickle
except Exception:
    import pickle

log = logging.getLogger(__name__)


class NoSqlManager(NamespaceManager):
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, expire=None, **params):
        NamespaceManager.__init__(self, namespace)

        if not url:
            raise MissingCacheParameter("url is required")

        if lock_dir:
            self.lock_dir = lock_dir
        elif data_dir:
            self.lock_dir = data_dir + "/container_tcd_lock"
        if hasattr(self, "lock_dir"):
            verify_directory(self.lock_dir)

        # Specify the serializer to use (pickle or json?)
        self.serializer = params.pop("serializer", "pickle")
        assert self.serializer in ["pickle", "json"]

        self._expiretime = int(expire) if expire else None

        conn_params = {}
        parts = url.split("?", 1)
        url = parts[0]
        if len(parts) > 1:
            conn_params = dict(p.split("=", 1) for p in parts[1].split("&"))

        host, port = url.split(":", 1)

        self.open_connection(host, int(port), **conn_params)

    def open_connection(self, host, port):
        self.db_conn = None

    def get_creation_lock(self, key):
        return file_synchronizer(identifier="tccontainer/funclock/%s" % self.namespace, lock_dir=self.lock_dir)

    def _format_key(self, key):
        return self.namespace + "_"

    def __getitem__(self, key):
        if self.serializer == "json":
            payload = self.db_conn.get(self._format_key(key))
            if payload is None:
                raise KeyError(key)
            if isinstance(payload, bytes):
                return json.loads(payload.decode("utf-8"))
            else:
                return json.loads(payload)
        else:
            payload = self.db_conn.get(self._format_key(key))
            if payload is None:
                raise KeyError(key)
            return pickle.loads(payload)

    def __contains__(self, key):
        return self.db_conn.has_key(self._format_key(key))

    def has_key(self, key):
        return key in self

    def set_value(self, key, value, expiretime=None):
        """
        expiretime is not used but necessary for beaker/container.py compatibility
        """
        if self.serializer == "json":
            self.db_conn[self._format_key(key)] = json.dumps(value, ensure_ascii=True)
        else:
            self.db_conn[self._format_key(key)] = pickle.dumps(value, 2)

    def __setitem__(self, key, value):
        self.set_value(key, value, self._expiretime)

    def __delitem__(self, key):
        del self.db_conn[self._format_key(key)]

    def do_remove(self):
        self.db_conn.clear()

    def keys(self):
        return self.db_conn.keys()


class NoSqlManagerContainer(Container):
    namespace_class = NoSqlManager
