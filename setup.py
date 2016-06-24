from setuptools import setup, find_packages
import sys, os

version = '0.2.0+dd.13'

TESTS_REQUIRE = ['nose']

setup(name='beaker_extensions',
      version=version,
      description="Beaker extensions for additional back-end stores.",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Didip Kerabat',
      author_email='didipk@gmail.com',
      url='',
      license='',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
      ],
      extras_require={
          'cassandra_cql': ['cassandra-driver>=3.1.0'],  # 3.1.0 added result iterators
          'testsuite': [TESTS_REQUIRE]
      },
      test_suite='nose.collector',
      tests_require=TESTS_REQUIRE,
      entry_points="""
      # -*- Entry points: -*-
      [beaker.backends]
      redis = beaker_extensions.redis_:RedisManager
      tyrant = beaker_extensions.tyrant_:TokyoTyrantManager
      riak = beaker_extensions.riak_:RiakManager
      dynomite = beaker_extensions.dynomite_:DynomiteManager
      ringo = beaker_extensions.ringo:RingoManager
      cassandra = beaker_extensions.cassandra_thrift:CassandraManager
      cassandra_cql = beaker_extensions.cassandra_cql:CassandraCqlManager
      """,
      )
