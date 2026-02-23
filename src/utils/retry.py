"""Retry-декоратор для обработки временных сбоев.

Предоставляет декораторы для синхронных и асинхронных функций
с настраиваемым количеством попыток, задержкой между ними
и фильтрацией типов исключений.

Пример использования:
    @async_retry(max_retries=3, delay=2.0, exceptions=(aiohttp.ClientError,))
    async def fetch_data(url: str) -> dict:
        ...

    @sync_retry(max_retries=3, delay=1.0)
    def read_file(path: str) -> str:
        ...
"""

import asyncio
import functools
from collections.abc import Callable
from typing import Any, TypeVar

from src.config import get_logger

logger = get_logger("retry")

F = TypeVar("F", bound=Callable[..., Any])


def async_retry(
    max_retries: int = 3,
    delay: float = 2.0,
    backoff_factor: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[F], F]:
    """Декоратор retry для асинхронных функций.

    При возникновении указанных исключений повторяет вызов функции
    с экспоненциально растущей задержкой между попытками.

    Args:
        max_retries: Максимальное количество повторных попыток.
        delay: Начальная задержка между попытками в секундах.
        backoff_factor: Множитель задержки после каждой попытки.
        exceptions: Кортеж типов исключений, при которых делать retry.

    Returns:
        Декорированная асинхронная функция с retry-логикой.

    Raises:
        Последнее пойманное исключение, если все попытки исчерпаны.
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: BaseException | None = None
            current_delay = delay

            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(
                            "retry_exhausted",
                            function=func.__name__,
                            attempt=attempt,
                            max_retries=max_retries,
                            error=str(e),
                            error_type=type(e).__name__,
                        )
                        raise

                    logger.warning(
                        "retry_attempt",
                        function=func.__name__,
                        attempt=attempt,
                        max_retries=max_retries,
                        next_delay=current_delay,
                        error=str(e),
                        error_type=type(e).__name__,
                    )

                    await asyncio.sleep(current_delay)
                    current_delay *= backoff_factor

            if last_exception is not None:
                raise last_exception

        return wrapper  # type: ignore[return-value]

    return decorator


def sync_retry(
    max_retries: int = 3,
    delay: float = 2.0,
    backoff_factor: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[F], F]:
    """Декоратор retry для синхронных функций.

    При возникновении указанных исключений повторяет вызов функции
    с экспоненциально растущей задержкой между попытками.

    Args:
        max_retries: Максимальное количество повторных попыток.
        delay: Начальная задержка между попытками в секундах.
        backoff_factor: Множитель задержки после каждой попытки.
        exceptions: Кортеж типов исключений, при которых делать retry.

    Returns:
        Декорированная синхронная функция с retry-логикой.

    Raises:
        Последнее пойманное исключение, если все попытки исчерпаны.
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: BaseException | None = None
            current_delay = delay

            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(
                            "retry_exhausted",
                            function=func.__name__,
                            attempt=attempt,
                            max_retries=max_retries,
                            error=str(e),
                            error_type=type(e).__name__,
                        )
                        raise

                    logger.warning(
                        "retry_attempt",
                        function=func.__name__,
                        attempt=attempt,
                        max_retries=max_retries,
                        next_delay=current_delay,
                        error=str(e),
                        error_type=type(e).__name__,
                    )

                    import time
                    time.sleep(current_delay)
                    current_delay *= backoff_factor

            if last_exception is not None:
                raise last_exception

        return wrapper  # type: ignore[return-value]

    return decorator
