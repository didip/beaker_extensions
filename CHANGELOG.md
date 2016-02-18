# 0.2.0+dd.6

*Backwards incompatible*: The cassandra_cql backend now creates its table with an `updated_at` column which it keeps up to date. This makes it easier to have an automated job clean up old sessions. You need to manually edit your existing tables before upgrading.
