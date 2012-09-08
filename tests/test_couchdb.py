# coding: utf-8
from beaker.cache import Cache
from beaker.exceptions import InvalidCacheBackendError
from beaker.middleware import CacheMiddleware
from nose import SkipTest
from webtest import TestApp

try:
    import couchdb
    from couchdb.http import ResourceNotFound
except ImportError:
    raise SkipTest("couchdb-python 0.8+ must be installed to perform this test")

db_url = 'localhost:5984'
db_type = 'couchdb'
db_name = 'beaker_extension_test'

server = couchdb.Server('http://' + db_url + '/')
try:
    server.delete(db_name)
except ResourceNotFound:
    pass
server.create(db_name)

def simple_app(environ, start_response):
    print "simple_app"
    extra_args = {}
    clear = False
    if environ.get('beaker.clear'):
        clear = True
    extra_args['type'] = db_type
    extra_args['url'] = db_url
    extra_args['data_dir'] = './cache'
    extra_args['database'] = db_name
    cache = environ['beaker.cache'].get_cache('testcache', **extra_args)
    if clear:
        cache.clear()
    try:
        value = cache.get_value('value')
    except:
        value = 0
    cache.set_value('value', value+1)
    start_response('200 OK', [('Content-type', 'text/plain')])
    return ['The current value is: %s' % cache.get_value('value')]

def cache_manager_app(environ, start_response):
    print "cache_manager_app"
    cm = environ['beaker.cache']
    cm.get_cache('test')['test_key'] = 'test value'

    start_response('200 OK', [('Content-type', 'text/plain')])
    yield "test_key is: %s\n" % cm.get_cache('test')['test_key']
    cm.get_cache('test').clear()

    try:
        test_value = cm.get_cache('test')['test_key']
    except KeyError:
        yield "test_key cleared"
    else:
        yield "test_key wasn't cleared, is: %s\n" % \
            cm.get_cache('test')['test_key']

def test_has_key():
    cache = Cache('test', url=db_url, type=db_type, database=db_name)
    o = object()
    cache.set_value("test", o)
    assert cache.has_key("test")
    assert "test" in cache
    assert not cache.has_key("foo")
    assert "foo" not in cache
    cache.remove_value("test")
    assert not cache.has_key("test")

def test_has_key_multicache():
    cache = Cache('test', url=db_url, type=db_type, database=db_name)
    o = object()
    cache.set_value("test", o)
    assert cache.has_key("test")
    assert "test" in cache
    cache = Cache('test', url=db_url, type=db_type, database=db_name)
    assert cache.has_key("test")
    cache.remove_value('test')

def test_clear():
    cache = Cache('test', url=db_url, type=db_type, database=db_name)
    o = object()
    cache.set_value("test", o)
    assert cache.has_key("test")
    cache.clear()
    assert not cache.has_key("test")

def test_unicode_keys():
    cache = Cache('test', url=db_url, type=db_type, database=db_name)
    o = object()
    cache.set_value(u'hiŏ', o)
    assert u'hiŏ' in cache
    assert u'hŏa' not in cache
    cache.remove_value(u'hiŏ')
    assert u'hiŏ' not in cache
    
def test_increment():
    app = TestApp(CacheMiddleware(simple_app))
    res = app.get('/', extra_environ={'beaker.clear':True})
    assert 'current value is: 1' in res
    res = app.get('/')
    assert 'current value is: 2' in res
    res = app.get('/')
    assert 'current value is: 3' in res

def test_cache_manager():
    app = TestApp(CacheMiddleware(cache_manager_app))
    res = app.get('/')
    assert 'test_key is: test value' in res
    assert 'test_key cleared' in res

