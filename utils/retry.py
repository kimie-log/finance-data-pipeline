import random
import time
from typing import Callable, Iterable, Tuple, TypeVar

from utils.logger import logger

T = TypeVar("T")


def run_with_retry(
    action: Callable[[], T],
    *,
    action_name: str,
    retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 30.0,
    backoff: float = 2.0,
    jitter: float = 0.2,
    retry_exceptions: Iterable[type[BaseException]] = (Exception,),
) -> T:
    """
    以指數退避重試執行指定動作。
    - retries: 失敗後可重試的次數（不含第一次）
    - jitter: 隨機抖動比例，避免同時重試造成尖峰
    """
    total_attempts = retries + 1
    retry_exceptions = tuple(retry_exceptions)

    for attempt in range(1, total_attempts + 1):
        try:
            return action()
        except retry_exceptions as exc:
            if attempt >= total_attempts:
                logger.exception(
                    "%s failed after %s attempts: %s",
                    action_name,
                    total_attempts,
                    exc,
                )
                raise

            delay = min(max_delay, initial_delay * (backoff ** (attempt - 1)))
            if jitter > 0:
                delay *= 1 + random.uniform(-jitter, jitter)
            delay = max(0.0, delay)

            logger.warning(
                "%s failed (attempt %s/%s): %s. Retrying in %.2fs...",
                action_name,
                attempt,
                total_attempts,
                exc,
                delay,
            )
            time.sleep(delay)
