from beaker.exceptions import InvalidCacheBackendError

try:
    from cassandra.policies import RetryPolicy
except ImportError:
    raise InvalidCacheBackendError(
        "cassandra_cql backend requires the 'cassandra-driver' library"
    )


class AlwaysRetryNextHostPolicy(RetryPolicy):
    def on_read_timeout(
        self,
        query,
        consistency,
        required_responses,
        received_responses,
        data_retrieved,
        retry_num,
    ):
        if retry_num == 0:
            return self.RETRY_NEXT_HOST, None
        else:
            return self.RETHROW, None

    def on_write_timeout(
        self,
        query,
        consistency,
        write_type,
        required_responses,
        received_responses,
        retry_num,
    ):
        if retry_num == 0:
            return self.RETRY_NEXT_HOST, None
        else:
            return self.RETHROW, None

    def on_unavailable(
        self, query, consistency, required_replicas, alive_replicas, retry_num
    ):
        if retry_num == 0:
            return self.RETRY_NEXT_HOST, None
        else:
            return self.RETHROW, None

    def on_request_error(self, query, consistency, error, retry_num):
        """
        Our goal here is to only allow one retry
        """
        if retry_num == 1:
            return self.RETRY_NEXT_HOST, None
        else:
            return self.RETHROW, None
