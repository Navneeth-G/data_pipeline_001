# pipeline_logic_scripts/utils/log_retry_decorators.py

import time
import functools
from typing import Callable, Tuple, Type


from utils.log_generator import log


def log_execution_time(func: Callable) -> Callable:
    """
    A decorator that logs the start, end, and execution time of a function
    using the custom PipelineLogger.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        log_key = f"Execution Timing: {func.__name__}"
        
        log.info(
            f"Starting execution of '{func.__name__}'.",
            log_key=log_key,
            status="STARTED"
        )
        
        start_time = time.monotonic()
        try:
            # Execute the original function
            result = func(*args, **kwargs)
            return result
        finally:
            # This 'finally' block ensures that the finish log is always recorded,
            # even if the wrapped function raises an error.
            end_time = time.monotonic()
            duration = end_time - start_time
            
            log.info(
                f"Finished execution of '{func.__name__}'.",
                log_key=log_key,
                status="FINISHED",
                duration_seconds=f"{duration:.4f}" # Format to 4 decimal places
            )
            
    return wrapper


def retry(max_attempts: int = 3, delay_seconds: int = 5, exceptions_to_catch: Tuple[Type[Exception], ...] = (Exception,)) -> Callable:
    """
    A standalone decorator to retry a function, with logging for each attempt.
    Its defaults are hardcoded and it has no external project dependencies.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 1:
                        log.info(
                            f"Function '{func.__name__}' succeeded on attempt {attempt}.",
                            log_key=f"Retryable Task: {func.__name__}",
                            status="SUCCESS_ON_RETRY"
                        )
                    return result
                except exceptions_to_catch as e:
                    log_key_for_retry = f"Retryable Task: {func.__name__}"
                    if attempt == max_attempts:
                        log.error(
                            f"Function '{func.__name__}' failed permanently after {max_attempts} attempts.",
                            log_key=log_key_for_retry,
                            status="FINAL_FAILURE",
                            error_type=type(e).__name__,
                            error_message=str(e)
                        )
                        raise
                    else:
                        log.warning(
                            f"Function '{func.__name__}' failed. Retrying in {delay_seconds}s.",
                            log_key=log_key_for_retry,
                            status="RETRYING",
                            attempt=f"{attempt}/{max_attempts}",
                            error_type=type(e).__name__,
                            error_message=str(e)
                        )
                        time.sleep(delay_seconds)
        return wrapper
    return decorator

