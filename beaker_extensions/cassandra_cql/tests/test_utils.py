from typing import TYPE_CHECKING
import unittest

from nose.plugins.attrib import attr

from beaker_extensions.cassandra_cql.utils import retry


if TYPE_CHECKING:
    from typing import Literal, NoReturn, Optional, Type


class ExampleException(Exception):
    pass


class OtherExampleException(Exception):
    pass


class UnhandledException(Exception):
    pass


class ExampleRetryable(object):
    _tries = 2
    _RETRYABLE_EXCEPTIONS = (ExampleException, OtherExampleException)

    def __init__(self):
        self.should_fail = True
        self.fail_counter = None  # type: Optional[int]
        self.call_counter = 0

    @retry
    def always_passes(self):  # type: () -> Literal[True]
        self.call_counter += 1
        return True

    @retry
    def always_fails(self, exception_class=ExampleException):  # type: (Type[Exception]) -> NoReturn
        self.call_counter += 1
        raise exception_class()

    @retry
    def alternate_fails(self):  # type: () -> Literal[True]
        self.call_counter += 1
        try:
            if self.should_fail:
                raise OtherExampleException()
        finally:
            self.should_fail = not self.should_fail
        return True

    @retry(tries=3)
    def fail_countdown_custom(self, bad_runs):  # type: (int) -> Literal[True]
        self.call_counter += 1
        if self.fail_counter is None:
            self.fail_counter = bad_runs
        self.fail_counter -= 1
        if self.fail_counter:
            raise ExampleException()
        return True


@attr("cassandra_cql")
class TestRetries(unittest.TestCase):
    def setUp(self):
        self.er = ExampleRetryable()

    def test_normal_success(self):
        x = self.er.always_passes()
        self.assertEquals(self.er.call_counter, 1)

    def test_normal_fail(self):
        # Never succeeds
        with self.assertRaises(ExampleException):
            self.er.always_fails()
        self.assertEquals(self.er.call_counter, 2)

    def test_cant_retry(self):
        # This exception is never retried
        with self.assertRaises(UnhandledException):
            self.er.always_fails(UnhandledException)
        self.assertEquals(self.er.call_counter, 1)

    def test_retries_eventually(self):
        # Fails once, and then succeeds on the 2nd try
        assert self.er.alternate_fails()
        self.assertEquals(self.er.call_counter, 2)

    def test_custom_retries_exceeded(self):
        # custom override to try 3 times, but function will fail 4 times
        with self.assertRaises(ExampleException):
            self.er.fail_countdown_custom(4)
        self.assertEquals(self.er.call_counter, 3)

    def test_custom_retries_success(self):
        # custom override to try 3 times, function succeeds on 3rd try
        assert self.er.fail_countdown_custom(3)
        self.assertEquals(self.er.call_counter, 3)
