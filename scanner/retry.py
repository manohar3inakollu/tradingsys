import time
from functools import wraps
from logger import setup_logger

log = setup_logger('layer1')


def retry(max_attempts: int = 3, delay: float = 2.0):
    """
    Decorator — retries a function up to max_attempts times.
    Delay doubles on each retry (exponential backoff).
    Raises the last exception if all attempts fail.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        log.error(
                            f"[retry] {func.__name__} failed after "
                            f"{max_attempts} attempts: {exc}"
                        )
                        raise
                    wait = delay * attempt
                    log.warning(
                        f"[retry] {func.__name__} attempt {attempt}/"
                        f"{max_attempts} failed: {exc} — "
                        f"retrying in {wait:.0f}s"
                    )
                    time.sleep(wait)
            raise last_exc
        return wrapper
    return decorator
