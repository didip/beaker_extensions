## Installation

```
pip install git+git://github.com/didip/beaker_extensions.git
```

## Usage

Eg:
```
beaker.session.type = tyrant
beaker.session.url = 127.0.0.1:1978
```

## Development

You can install the library in dev mode for most backends with
```
pip install -e .
```
You'll have to install the underlying libraries for the backends yourself.

The cassandra_cql backend lists its dependency as an `extras_require` so to pull that in use
```
pip install -e .[cassandra_cql]
```

## Tests

*Warning: the tests talk to real data stores running on localhost and may overwrite or delete data!*

```
nosetests
```

## Credits

Thanks to Jack Hsu for providing the tokyo example:
http://www.jackhsu.com/2009/05/27/pylons-with-tokyo-cabinet-beaker-sessions
