# coding=utf-8

from __future__ import absolute_import
import time

from beaker_extensions._compat import u_
from nose.tools import assert_raises


class CommonMethodMixin(object):
    """
    These test methods will get mixed into classes specific to each backend.

    Many tests are adapted from beaker/tests/test_cache.py
    """

    def test_set(self):
        # via set_value
        assert "foo" not in self.cache
        self.cache.set_value("foo", "bar")
        assert "foo" in self.cache
        assert self.cache.get_value("foo") == "bar"

        # via dict interface
        assert "key2" not in self.cache
        self.cache["key2"] = "val2"
        assert "key2" in self.cache
        assert self.cache.get_value("key2") == "val2"

    def test_get(self):
        assert "foo" not in self.cache
        self.cache.set_value("foo", "bar")
        assert "foo" in self.cache
        assert self.cache.get_value("foo") == "bar"
        assert self.cache["foo"] == "bar"

    def test_get_missing(self):
        assert "foo" not in self.cache
        with assert_raises(KeyError):
            self.cache.get_value("foo")
        with assert_raises(KeyError):
            self.cache["foo"]

    def test_update(self):
        self.cache.set_value("foo", "bar")
        assert self.cache.get_value("foo") == "bar"

        # via set_value
        self.cache.set_value("foo", "omgwtfbbq")
        assert self.cache.get_value("foo") == "omgwtfbbq"

        # via dict interface
        self.cache["foo"] = "pizza time!!"
        assert self.cache.get_value("foo") == "pizza time!!"

    def test_remove(self):
        # via remove_value
        self.cache.set_value("foo", "bar")
        assert self.cache.get_value("foo") == "bar"
        self.cache.remove_value("foo")
        assert "foo" not in self.cache

        # via del
        self.cache.set_value("key2", "val2")
        assert self.cache.get_value("key2") == "val2"
        del self.cache["key2"]
        assert "key2" not in self.cache

    def test_nonexistant(self):
        try:
            self.cache.get_value("foo")
            assert False, "get_value should have raised an exception"
        except KeyError:
            pass
        assert "foo" not in self.cache

    def test_has_key(self):
        cache = self.cache
        cache.set_value("test", 42)
        assert cache.has_key("test")
        assert "test" in cache
        assert not cache.has_key("foo")
        assert "foo" not in cache
        cache.remove_value("test")
        assert not cache.has_key("test")

    def test_expire_changes(self):
        cache = self.cache
        cache.set_value("test", 10)
        assert cache.has_key("test")
        assert cache["test"] == 10

        # ensure that we can change a never-expiring value
        cache.set_value("test", 20, expiretime=1)
        assert cache.has_key("test")
        assert cache["test"] == 20
        time.sleep(1)
        assert not cache.has_key("test")

        # test that we can change it before its expired
        cache.set_value("test", 30, expiretime=50)
        assert cache.has_key("test")
        assert cache["test"] == 30

        cache.set_value("test", 40, expiretime=3)
        assert cache.has_key("test")
        assert cache["test"] == 40
        time.sleep(3)
        assert not cache.has_key("test")

    def test_fresh_createfunc(self):
        cache = self.cache
        x = cache.get_value("test", createfunc=lambda: 10, expiretime=2)
        assert x == 10
        x = cache.get_value("test", createfunc=lambda: 12, expiretime=2)
        assert x == 10
        x = cache.get_value("test", createfunc=lambda: 14, expiretime=2)
        assert x == 10
        time.sleep(2)
        x = cache.get_value("test", createfunc=lambda: 16, expiretime=2)
        assert x == 16
        x = cache.get_value("test", createfunc=lambda: 18, expiretime=2)
        assert x == 16

        cache.remove_value("test")
        assert "test" not in cache
        x = cache.get_value("test", createfunc=lambda: 20, expiretime=2)
        assert x == 20

    def test_unicode_keys(self):
        cache = self.cache
        cache.set_value(u_("hiŏ"), 42)
        assert u_("hiŏ") in cache
        assert u_("hŏa") not in cache
        cache.remove_value(u_("hiŏ"))
        assert u_("hiŏ") not in cache

    def test_remove_stale(self):
        """test that remove_value() removes even if the value is expired."""

        cache = self.cache
        cache.namespace[b"key"] = (time.time() - 60, 5, "hi")
        container = cache._get_value("key")
        assert not container.has_current_value()
        assert b"key" in container.namespace
        cache.remove_value("key")
        assert b"key" not in container.namespace

        # safe to call again
        cache.remove_value("key")

    def test_multi_keys(self):
        cache = self.cache
        cache.clear()
        called = {}

        def create_func():
            called["here"] = True
            return "howdy"

        try:
            cache.get_value("key1")
        except KeyError:
            pass
        else:
            raise Exception("Failed to keyerror on nonexistent key")

        assert "howdy" == cache.get_value("key2", createfunc=create_func)
        assert called["here"] == True
        del called["here"]

        try:
            cache.get_value("key3")
        except KeyError:
            pass
        else:
            raise Exception("Failed to keyerror on nonexistent key")
        try:
            cache.get_value("key1")
        except KeyError:
            pass
        else:
            raise Exception("Failed to keyerror on nonexistent key")

        assert "howdy" == cache.get_value("key2", createfunc=create_func)
        assert called == {}

    def test_store_obj(self):
        # via set_value
        err = Exception("Too much partying")
        assert "err" not in self.cache
        self.cache.set_value("err", err)
        assert "err" in self.cache
        got = self.cache.get_value("err")
        assert type(got) == Exception
        assert got.args[0] == "Too much partying"

        # via dict interface
        assert "key2" not in self.cache
        self.cache["key2"] = err
        assert "key2" in self.cache
        got2 = self.cache.get_value("key2")
        assert type(got2) == Exception
        assert got2.args[0] == "Too much partying"
