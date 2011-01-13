from setuptools import setup, find_packages
import sys, os

version = '0.1'

tests_require = ['nose', 'webtest']

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
      test_suite='nose.collector',
      tests_require=tests_require,
      entry_points="""
      # -*- Entry points: -*-
      [beaker.backends]
      dynomite = beaker_extensions.dynomite_:DynomiteManager
      redis = beaker_extensions.redis_:RedisManager
      ringo = beaker_extensions.ringo:RingoManager
      tyrant = beaker_extensions.tyrant_:TokyoTyrantManager
      couchdb = beaker_extensions.couchdb_:CouchDBManager
      """,
      )
