import logging
from random import random
from typing import TYPE_CHECKING, TypedDict, Protocol
import weakref

from datadog import statsd


log = logging.getLogger(__name__)


if TYPE_CHECKING:
    from cassandra.cluster import Cluster
    from typing import Text, Literal, Optional, List

    class _StatsdService(Protocol):
        def increment(
            self,
            metric,  # type: Text
            tags=None,  # type: Optional[List[str]]
        ):
            pass

        def gauge(
            self,
            metric,  # type: Text
            value,  # type: float
            sample_rate=None,  # type: Optional[float]
        ):
            pass

        def distribution(
            self,
            metric,  # type: Text
            value,  # type: float
        ):
            pass

    ResponseStatus = Literal[
        "connection_error",
        "ignore",
        "other_error",
        "read_timeout",
        "retry",
        "unavailable",
        "write_timeout",
    ]


class _RequestTimer(object):
    def __init__(
        self,
        statsd,  # type: _StatsdService
        metric,  # type: Text
    ):
        self._statsd = statsd
        self._metric = metric

    def addValue(
        self,
        value,  # type: float
    ):
        try:
            self._statsd.distribution(self._metric, value)
        except Exception:
            log.debug("Error occurred while submitting Cassandra request timing: ignoring", exc_info=True)


class _SupportsMetrics(Protocol):
    """A protocol representing all of the hooks exposed by cassandra.metrics.Metrics"""

    @property
    def request_timer(self):  # type: (...) -> _RequestTimer
        return _RequestTimer(statsd, "")

    def on_connection_error(self):
        pass

    def on_write_timeout(self):
        pass

    def on_read_timeout(self):
        pass

    def on_unavailable(self):
        pass

    def on_other_error(self):
        pass

    def on_ignore(self):
        pass

    def on_retry(self):
        pass


_ClusterStats = TypedDict("_ClusterStats", {"known_hosts": int, "connected_hosts": int, "open_connections": int})


class StatsdMetrics:
    """StatsdMetrics is intended to be a drop-in replacement for cassandra.metrics.Metrics for a Cluster instance."""

    _STATS_PREFIX = "dd.cassandra_cql.driver."

    def __init__(
        self,
        cluster_proxy,  # type: weakref.ProxyType[Cluster]
        stats_service=statsd,  # type: _StatsdService
        cluster_stats_sample_rate=0.0002,  # type: float  # one in five-thousand
    ):
        self._cluster_proxy = cluster_proxy
        self._statsd = stats_service
        self._request_timer = _RequestTimer(self._statsd, self._STATS_PREFIX + "response.latency")
        self._cluster_stats_sample_rate = cluster_stats_sample_rate

    @classmethod
    def bind_ref(
        cls,
        cluster,  # type: Cluster
    ):  # type: (...) -> _SupportsMetrics
        instance = cls(weakref.proxy(cluster))
        return instance

    def _on_response(
        self,
        status,  # type: ResponseStatus
    ):
        try:
            self._statsd.increment(self._STATS_PREFIX + "response", tags=["status:%s" % status])
        except Exception:
            log.debug("Error occurred while submitting Cassandra response stats: ignoring", exc_info=True)

    def on_connection_error(self):
        """An unrecoverable error was hit when attempting to use a connection,
        or the connection was already closed or defunct.
        """
        self._on_response("connection_error")

    def on_ignore(self):
        """A query failed and we're not retrying it or re-throwing the error,
        we're just pretending that nothing happened.
        """
        self._on_response("ignore")

    def on_other_error(self):
        """Cluster is overloaded or still bootstrapping, or a truncate operation
        failed, or some other server error occurred.
        """
        self._on_response("other_error")

    def on_read_timeout(self):
        """Cluster-defined read timeout has expired"""
        self._on_response("read_timeout")

    def on_retry(self):
        """A query is being retried against the same or a new host"""
        self._on_response("retry")

    def on_unavailable(self):
        """The required cluster members for a query a not available"""
        self._on_response("unavailable")

    def on_write_timeout(self):
        """Cluster-defined write timeout has expired"""
        self._on_response("write_timeout")

    def _submit_cluster_gauges(self):
        # NOTE: These stats don't often change, and they're relatively expensive
        # to calculate given the utility that they provide. For these reasons
        # we'll only bother to tabulate and submit them given some random
        # sampling.
        if not random() < self._cluster_stats_sample_rate:
            return

        stats = self._generate_cluster_stats()
        self._statsd.gauge(self._STATS_PREFIX + "num_known_hosts", stats["known_hosts"], sample_rate=1)
        self._statsd.gauge(self._STATS_PREFIX + "num_connected_hosts", stats["connected_hosts"], sample_rate=1)
        self._statsd.gauge(self._STATS_PREFIX + "num_open_connections", stats["open_connections"], sample_rate=1)

    def _generate_cluster_stats(self):  # type: (...) -> _ClusterStats
        known_hosts = len(self._cluster_proxy.metadata.all_hosts())
        connected_hosts = set()
        open_connections = 0
        for session in self._cluster_proxy.sessions:
            for host, pool in session._pools.items():
                connected_hosts.add(host)
                open_connections += pool.open_count

        return _ClusterStats(
            known_hosts=known_hosts, connected_hosts=len(connected_hosts), open_connections=open_connections
        )

    @property
    def request_timer(self):
        # NOTE: Accessing the request_timer represents the finalization of a query
        # in the driver. We'll use this opportunity to submit statistics about our
        # cluster state.
        try:
            self._submit_cluster_gauges()
        except Exception:
            log.debug("Error occurred while submitting Cassandra cluster stats: ignoring", exc_info=True)

        return self._request_timer
