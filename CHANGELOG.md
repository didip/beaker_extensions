# 0.4.0+dd.0

- optionally report cassandra metrics to statsd by passing
  `cluster_metrics_enabled=True` to the `CassandraCqlManager`.

# 0.3.0+dd.5
- allow cassandra query timeout to be set as `float`
- depend on non-datadog [beaker in same version as Dogweb](https://github.com/DataDog/dogweb/blob/prod/requirements.in#L102) 

# 0.3.0+dd.0
...

# 0.2.0+dd.27

Add support for username and password authentication to `CassandraCQLManager`

# 0.2.0+dd.26

Hello Friend,

- Added formatting.
- Added testing.
- Added ability to run tests locally.
- Updated required beaker version.

# 0.2.0+dd.18

Remove the `ddtrace` option from `CassandraCqlManager` (deprecated in favor of `ddtrace` monkey patching).

# 0.2.0+dd.17

Add an option to `CassandraCqlManager` to trace queries with [ddtrace](https://github.com/Datadog/dd-trace-py)

# 0.2.0+dd.16

Fix cassandra_cql missing value handling.

Move retry support into only the cassandra_cql backend; it needs to only catch exceptions specific to the backend.

Remove dependency on retry lib.

Add a cassandra_cql retry policy to try another host.

# 0.2.0+dd.15

(dd.14 was a broken release)

The cassandra_cql backend no longer uses socket.TCP_NODELAY and allows a configurable query timeout.

All backends that use the NoSqlManager have a new `tries` setting which will retry operations on failure.

# 0.2.0+dd.13

The cassandra_cql backend uses socket.TCP_NODELAY.

# 0.2.0+dd.12

The cassandra_cql backend no longer uses quorum consistency level for reads and writes. We're concerned about the performance impact and don't think we actually need this guarantee.

# 0.2.0+dd.11

The expire option now works with the cassandra_cql backend.

The cassandra_cql backend no longer uses the explicit `updated_at` column and won't create it anymore when creating a table. If you want to query on rows in this way, you can use cql's row timestamp mechanism.

# 0.2.0+dd.10

The cassandra_cql backend uses quorum consistency level for reads and writes.

# 0.2.0+dd.9

The cassandra_cql backend will handle the newer error message when checking if the table exists and it doesn't.

# 0.2.0+dd.8

The cassandra_cql backend is upgraded for, and requires, cassandra-driver >= 3.1.0.

# 0.2.0+dd.7

The cassandra_cql backend now handles its max_schema_agreement_wait config option properly. Previously it would pass it as a str to the cassandra driver which wouldn't like that one bit.

# 0.2.0+dd.6

*Backwards incompatible*: The cassandra_cql backend now creates its table with an `updated_at` column which it keeps up to date. This makes it easier to have an automated job clean up old sessions. You need to manually edit your existing tables before upgrading.
