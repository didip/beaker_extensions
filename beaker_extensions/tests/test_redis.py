from __future__ import absolute_import
import unittest

from nose.plugins.attrib import attr
from nose.tools import nottest

from beaker.cache import Cache

from .common import CommonMethodMixin


class RedisTestOverrides(object):
    @nottest
    def test_fresh_createfunc(self):
        # createfunc depends on create_lock being implemented which it isn't for
        # redis, so skip this test which would otherwise run from
        # CommonMethodMixin.
        pass

    @nottest
    def test_multi_keys(self):
        # This also uses createfunc which it isn't for redis, so skip
        # this test which would otherwise run from CommonMethodMixin.
        pass


@attr("redis")
class TestRedisPickle(RedisTestOverrides, CommonMethodMixin, unittest.TestCase):
    def setUp(self):
        self.cache = Cache("testns", type="ext:redis", url="redis://localhost:6379", serializer="pickle")
        self.cache.clear()


@attr("redis")
class TestRedisJson(RedisTestOverrides, CommonMethodMixin, unittest.TestCase):
    def setUp(self):
        self.cache = Cache("testns", type="ext:redis", url="redis://localhost:6379", serializer="json")
        self.cache.clear()

    @nottest
    def test_store_obj(self):
        # We can't store objects with the json serializer so skip this test from
        # the CommonMethodMixin.
        pass
