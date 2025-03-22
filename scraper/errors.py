"""
Centralized error handling framework for the scraper.
"""

import logging
import functools
import traceback
from typing import Callable, TypeVar, Any, Optional

logger = logging.getLogger(__name__)

# Type variables for function signatures
F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")


class ScraperError(Exception):
    """Base exception class for all scraper errors."""

    pass


class ConfigError(ScraperError):
    """Error raised when configuration is invalid."""

    pass


class NetworkError(ScraperError):
    """Error raised when network requests fail."""

    pass


class ParsingError(ScraperError):
    """Error raised when content parsing fails."""

    pass


class StorageError(ScraperError):
    """Error raised when storage operations fail."""

    pass


class GeocodingError(ScraperError):
    """Error raised when geocoding operations fail."""

    pass


def handle_exceptions(
    reraise: bool = False, default_return: Any = None, error_type: Optional[type] = None
) -> Callable[[F], F]:
    """
    Decorator to handle exceptions in a consistent way.

    Args:
        reraise: Whether to reraise the exception after logging
        default_return: Default value to return if exception occurs
        error_type: Custom error type to wrap the original exception

    Returns:
        Decorated function
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Get full traceback
                tb = traceback.format_exc()

                # Log detailed error
                logger.error(f"Error in {func.__name__}: {str(e)}\n{tb}")

                if error_type and reraise:
                    # Wrap in specific error type
                    raise error_type(str(e)) from e
                elif reraise:
                    # Re-raise original exception
                    raise

                return default_return

        return wrapper

    return decorator


def retry(
    max_attempts: int = 3,
    delay_seconds: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable[[F], F]:
    """
    Decorator to retry a function on failure.

    Args:
        max_attempts: Maximum number of retry attempts
        delay_seconds: Initial delay between attempts
        backoff_factor: Factor by which delay increases each attempt
        exceptions: Tuple of exception types to catch and retry

    Returns:
        Decorated function
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import time

            attempt = 1
            current_delay = delay_seconds

            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        logger.error(
                            f"All {max_attempts} attempts failed for {func.__name__}: {str(e)}"
                        )
                        raise

                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {current_delay:.2f} seconds..."
                    )

                    time.sleep(current_delay)
                    current_delay *= backoff_factor
                    attempt += 1

        return wrapper

    return decorator
