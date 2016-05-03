# 0.2.0+dd.11

The expire option now works with the cassandra_cql backend.

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
