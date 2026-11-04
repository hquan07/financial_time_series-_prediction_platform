"""Bounded retry policy for transient provider errors only."""

from collections.abc import Callable

import requests
from curl_cffi.requests import exceptions as curl_errors
from tenacity import Retrying, retry_if_exception, stop_after_attempt
from tenacity.wait import wait_base, wait_random_exponential


class SourceError(Exception):
    """Sanitized source failure; never include request URLs or response bodies."""


class SourceSchemaError(SourceError):
    pass


class RetryableHTTPError(SourceError):
    def __init__(self, status_code: int, retry_after: float | None = None):
        super().__init__(f"retryable HTTP {status_code}")
        self.retry_after = retry_after


class PermanentHTTPError(SourceError):
    def __init__(self, status_code: int):
        super().__init__(f"permanent HTTP {status_code}")


def is_retryable(error: BaseException) -> bool:
    return isinstance(
        error,
        (
            RetryableHTTPError,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            curl_errors.Timeout,
            curl_errors.ConnectionError,
        ),
    )


class ProviderWait(wait_base):
    def __init__(self) -> None:
        self.fallback = wait_random_exponential(multiplier=1, max=8)

    def __call__(self, retry_state) -> float:
        error = retry_state.outcome.exception()
        if isinstance(error, RetryableHTTPError) and error.retry_after is not None:
            return max(0.0, min(error.retry_after, 60.0))
        return self.fallback(retry_state)


def fetch_with_retry[T](operation: Callable[[], T]) -> T:
    for attempt in Retrying(
        retry=retry_if_exception(is_retryable),
        stop=stop_after_attempt(3),
        wait=ProviderWait(),
        reraise=True,
    ):
        with attempt:
            return operation()
    raise AssertionError("retry loop did not return")
