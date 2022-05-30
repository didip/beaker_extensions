import weakref

try:
    from unittest.mock import Mock, call
except ImportError:
    from mock import Mock, call  # type: ignore

from cassandra.cluster import Cluster

from beaker_extensions.cassandra_cql.metrics import StatsdMetrics


def get_test_instance():
    statsd = Mock()
    cluster = Mock()
    cluster.metadata.all_hosts.return_value = []
    cluster.sessions = []
    metrics = StatsdMetrics(weakref.proxy(cluster), statsd)  # type: ignore
    return metrics, statsd, cluster


def assert_gauge_calls(mok, known_hosts=0, connected_hosts=0, open_connections=0):
    mok.gauge.assert_has_calls(
        [
            call("dd.cassandra_cql.driver.num_known_hosts", known_hosts, sample_rate=1),
            call("dd.cassandra_cql.driver.num_connected_hosts", connected_hosts, sample_rate=1),
            call("dd.cassandra_cql.driver.num_open_connections", open_connections, sample_rate=1),
        ]
    )


class TestStatsdMetrics(object):
    def test_instantiates_without_exploding(self):
        assert StatsdMetrics(weakref.proxy(Cluster()))

    def test_bind_ref_instantiates_without_exploding(self):
        assert StatsdMetrics.bind_ref(Cluster())

    def test_on_connection_error(self):
        metrics, statsd, _ = get_test_instance()

        metrics.on_connection_error()

        statsd.increment.assert_called_once_with("dd.cassandra_cql.driver.response", tags=["status:connection_error"])

    def test_on_ignore(self):
        metrics, statsd, _ = get_test_instance()

        metrics.on_ignore()

        statsd.increment.assert_called_once_with("dd.cassandra_cql.driver.response", tags=["status:ignore"])

    def test_on_other_error(self):
        metrics, statsd, _ = get_test_instance()

        metrics.on_other_error()

        statsd.increment.assert_called_once_with("dd.cassandra_cql.driver.response", tags=["status:other_error"])

    def test_on_read_timeout(self):
        metrics, statsd, _ = get_test_instance()

        metrics.on_read_timeout()

        statsd.increment.assert_called_once_with("dd.cassandra_cql.driver.response", tags=["status:read_timeout"])

    def test_on_retry(self):
        metrics, statsd, _ = get_test_instance()

        metrics.on_retry()

        statsd.increment.assert_called_once_with("dd.cassandra_cql.driver.response", tags=["status:retry"])

    def test_on_unavailable(self):
        metrics, statsd, _ = get_test_instance()

        metrics.on_unavailable()

        statsd.increment.assert_called_once_with("dd.cassandra_cql.driver.response", tags=["status:unavailable"])

    def test_on_write_timeout(self):
        metrics, statsd, _ = get_test_instance()

        metrics.on_write_timeout()

        statsd.increment.assert_called_once_with("dd.cassandra_cql.driver.response", tags=["status:write_timeout"])

    def test_request_timer(self):
        metrics, statsd, _ = get_test_instance()
        metrics._cluster_stats_sample_rate = 1.0
        value = 0.5

        metrics.request_timer.addValue(value)

        statsd.distribution.assert_called_once_with("dd.cassandra_cql.driver.response.latency", value)
        assert_gauge_calls(statsd)

    def test_cluster_gauges(self):
        class Pool(object):
            open_count = 3

        class Session(object):
            _pools = {1: Pool(), 2: Pool()}

        cluster = Cluster()
        cluster.metadata._hosts = {1: "host_1", 2: "host_2"}  # type: ignore
        cluster.sessions = [Session(), Session()]  # type: ignore

        statsd = Mock()
        metrics = StatsdMetrics(weakref.proxy(cluster), statsd, cluster_stats_sample_rate=1.0)

        metrics._submit_cluster_gauges()

        assert_gauge_calls(statsd, known_hosts=2, connected_hosts=2, open_connections=12)
