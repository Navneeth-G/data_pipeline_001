# pipeline_logic_scripts/utils/log_generator.py

import logging
import inspect
import pendulum
import sys


class PipelineLogger:
    """
    A standalone, custom logger class that creates structured, context-aware
    log messages. It now includes all standard logging levels.
    """
    def __init__(self, logger_name: str = "PipelineLogger", max_depth: int = 3):
        self.logger = logging.getLogger(logger_name)
        self.max_depth = max_depth

    def _get_caller_info(self) -> str:
        # ... (This function's logic remains the same)
        stack = inspect.stack()
        trace = []
        try:
            for frame_info in stack[1:self.max_depth + 1]:
                full_path = frame_info.filename; filename = full_path.split("/")[-1].split("\\")[-1]
                function = frame_info.function
                if "logging_setup.py" not in filename and "pipeline_decorators.py" not in filename:
                    trace.append(f"{filename}::{function}")
        finally:
            del stack
        return " -> ".join(reversed(trace)) if trace else "unknown_caller"

    def _format_log(self, message: str, timezone: str, **kwargs) -> str:
        # ... (This function's logic remains the same)
        log_key = kwargs.pop('log_key', None); status = kwargs.pop('status', None)
        caller = self._get_caller_info()
        now_utc = pendulum.now("UTC"); utc_timestamp_str = now_utc.to_iso8601_string()
        try:
            local_timestamp_str = now_utc.in_timezone(timezone).to_iso8601_string()
        except Exception:
            local_timestamp_str = f"Invalid Timezone ('{timezone}')"
        log_block = f"\n+-------------------- LOG START --------------------+"
        if log_key: log_block += f"\n| Key:       [ {log_key} ]"
        if status:  log_block += f"\n| Status:    [ {status} ]"
        log_block += f"\n| Timestamp: UTC: {utc_timestamp_str} | {timezone}: {local_timestamp_str}"
        log_block += f"\n| Caller:    {caller}"
        log_block += f"\n| Message:   {message}"
        if kwargs:
            log_block += "\n| Details:"
            for key, value in kwargs.items():
                log_block += f"\n|   - {key}: {value}"
        log_block += f"\n+--------------------- LOG END ---------------------+"
        return log_block

    def info(self, message: str = "Informational log event.", timezone: str = "America/Los_Angeles", **kwargs):
        self.logger.info(self._format_log(message, timezone, **kwargs))

    def warning(self, message: str = "Warning log event.", timezone: str = "America/Los_Angeles", **kwargs):
        self.logger.warning(self._format_log(message, timezone, **kwargs))

    def error(self, message: str = "Error log event.", timezone: str = "America/Los_Angeles", **kwargs):
        self.logger.error(self._format_log(message, timezone, **kwargs))
    
    def critical(self, message: str = "Critical error event.", timezone: str = "America/Los_Angeles", **kwargs):
        """Logs a message with level CRITICAL for very severe errors."""
        self.logger.critical(self._format_log(message, timezone, **kwargs))

    def exception(self, message: str = "An exception was caught and logged.", timezone: str = "America/Los_Angeles", **kwargs):
        """
        Logs a message with level ERROR and automatically appends exception traceback.
        This method should only be called from an 'except' block.
        """
        # The underlying .exception() method automatically handles adding the traceback.
        self.logger.exception(self._format_log(message, timezone, **kwargs))


def setup_pipeline_logger(logger_name: str = "PipelineLogger", level: int = logging.INFO, fmt: str = "%(asctime)s - %(levelname)-8s - %(message)s"):
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    if not logger.handlers:
        stream_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(fmt)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return PipelineLogger(logger_name=logger_name)

log = setup_pipeline_logger()
