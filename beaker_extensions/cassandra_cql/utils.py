from functools import partial, wraps
import logging
from typing import TYPE_CHECKING, Any, Callable, Protocol, TypeVar, cast, overload
from datadog import statsd

if TYPE_CHECKING:
    from typing import Optional, Type, Union

log = logging.getLogger(__name__)


class SupportsRetry(Protocol):
    _tries = 0  # type: int
    _RETRYABLE_EXCEPTIONS = tuple()  # type: tuple[Type[Exception], ...]


F = TypeVar("F", bound=Callable[..., Any])
Decorator = Callable[[F], F]

# This signature is for setting the tries override
@overload
def retry(
    func=None,  # type: None
    tries=None,  # type: Optional[int]
):  # type: (...) -> Decorator
    pass


# This signature is for wrapping a function
@overload
def retry(
    func,  # type: F
):  # type: (...) -> F
    pass


def retry(
    func=None,  # type: Optional[F]
    tries=None,  # type: Optional[int]
):  # type: (...) -> Union[F, Decorator[F]]
    """_retry will call the decorated function a number of times if a registered Exception is raised

    Args:
        tries (int, optional): The number of times to try the function. Defaults to the Class attribute `_tries`.
    """
    if func is None:
        return cast(Decorator[F], partial(retry, tries=tries))

    @wraps(func)
    def wrapper(
        self,  # type: SupportsRetry
        *args,
        **kwargs
    ):
        retry_count = tries or self._tries
        _tries = retry_count
        while _tries:
            try:
                return func(self, *args, **kwargs)
            except self._RETRYABLE_EXCEPTIONS:
                _tries -= 1
                if not _tries:
                    statsd.increment(
                        "dd.cassandra_cql.retry",
                        tags=[
                            "function:{}".format(func.__name__),
                            "status:error",
                            "retry_count:{}".format(_tries),
                        ],
                    )
                    raise
                t = retry_count - _tries
                statsd.increment(
                    "dd.cassandra_cql.retry",
                    tags=[
                        "function:{}".format(func.__name__),
                        "status:retry",
                        "retry_count:{}".format(_tries),
                    ],
                )
                log.warning(
                    "Caught retryable exception on try=%d (stack " "trace below). Retrying.",
                    t,
                    exc_info=True,
                )

    return cast(F, wrapper)
