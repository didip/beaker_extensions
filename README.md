## Usage

Install directly from git using PIP:

```
pip install git+git://github.com/didip/beaker_extensions.git
```

Now you can use the couchdb, redis, tyrant, riak, dynomite, and ringo extensions.

beaker.session.type = tyrant
beaker.session.url = 127.0.0.1:1978

Thanks to Jack Hsu for providing the tokyo example:
http://www.jackhsu.com/2009/05/27/pylons-with-tokyo-cabinet-beaker-sessions

== Information for CouchDB backend =============================================

beaker.session.type = couchdb
beaker.session.url = localhost:5984
beaker.session.database = project_session_dbname

Note:
    The CouchDB backend is modified from the redis_.py backend. Included test
script is modified from test_database.py in beaker library. To run the test,
CouchDB must be at localhost:5984 and in admin party mode. Test script will
attempt to create a database named 'beaker_extension_test'.
    Documents of sessions have keys similar to 'beaker:sha-hash:session', and
'type' fields set to 'beaker.session'.
