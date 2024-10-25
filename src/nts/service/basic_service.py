"""Provides SimpleService worker class for running as a daemon in the background."""

from typing import Union
import signal
import sys
import time
from datetime import datetime, timezone
import logging

from .__helpers import time_ms

try:
    import systemd.daemon  # type: ignore
except ModuleNotFoundError:
    pass


class CustomConsoleFormatter(logging.Formatter):
    """Console formatter for log records"""

    def __init__(
        self,
        service_name: str,
        worker_id: Union[int, None] = None,
        fmt: Union[str, None] = None,
        datefmt: Union[str, None] = None,
    ) -> None:
        if worker_id is None:
            worker_id = 1
        if fmt is None:
            fmt = "%(utc_timestamp)s - %(levelname)s - [%(worker_name)s] - %(message)s"
        super().__init__(fmt, datefmt)
        self.worker_name: str = f"{service_name}:{worker_id}"

    def format(self, record: logging.LogRecord) -> str:
        # Add the worker_id to the log record
        record.worker_name = self.worker_name

        record.utc_timestamp = datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S %Z"
        )
        # Convert the log level to a 3-letter abbreviation
        record.levelname = self.level_abbreviation(record.levelno)

        # Call the parent class's format method to do the actual formatting
        return super().format(record)

    @staticmethod
    def level_abbreviation(log_level: int) -> str:
        """Map logging levels to 3-letter abbreviations"""
        level_map = {
            logging.DEBUG: "DBG",
            logging.INFO: "INF",
            logging.WARNING: "WRN",
            logging.ERROR: "ERR",
            logging.CRITICAL: "CRT",
            logging.FATAL: "FTL",
        }
        return level_map.get(log_level, f"{log_level:03d}")


class BasicService:
    """
    SimpleService is a worker daemon compatible with systemd,
    can run in the background, and gracefully finishes on SIGTERM and SIGINT.
    """

    # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        service_name: str = "service",
        version: str = "0.0.1",
        delay: float = 5,
        **kwargs,
    ) -> None:
        self.__service_name: str = service_name
        self.__worker_id: int = kwargs.get("worker_id", 1)
        self.__version: str = version
        self.__delay: float = delay
        self.__logging_level: int = logging.DEBUG
        if "logging_level" in kwargs:
            try:
                if not isinstance(logging.getLevelName(kwargs["logging_level"]), int):
                    self.__logging_level = logging.DEBUG
                else:
                    self.__logging_level = logging.getLevelName(kwargs["logging_level"])
            except (TypeError, ValueError):
                pass
        self._logger: logging.Logger = logging.getLogger(__name__)
        self._logger.setLevel(self.logging_level)
        stdout_handler = logging.StreamHandler()
        stdout_handler.setLevel(self.logging_level)
        formatter = CustomConsoleFormatter(
            service_name=self.service_name, worker_id=self.worker_id
        )
        stdout_handler.setFormatter(formatter)
        self._logger.addHandler(stdout_handler)
        self._logger_add_custom_handlers()
        self._exit: bool = False

        signal.signal(signal.SIGTERM, self._handle_sigterm)

        if "systemd.daemon" in sys.modules:
            systemd.daemon.notify("READY=1")

        self.last_loop_timestamp_ms = time_ms()

    def _logger_add_custom_handlers(self) -> None:
        """Override this method to add custom handlers to logger"""

    @property
    def worker_id(self) -> int:
        """Worker id number"""
        return self.__worker_id

    @property
    def delay(self) -> float:
        """Service main loop sleeping time."""
        return self.__delay

    @delay.setter
    def delay(self, dt: float) -> None:
        if float(dt) < 0:
            self.logger.error("Delay must be >=0, got %g", float(dt))
        else:
            self.__delay = float(dt)
            self.logger.debug("Delay changed to %g", self.delay)

    @property
    def version(self) -> str:
        """Service version string."""
        return self.__version

    @property
    def service_name(self) -> str:
        """Service name string."""
        return self.__service_name

    @property
    def logger(self) -> logging.Logger:
        """Service logger."""
        return self._logger

    @property
    def logging_level(self) -> int:
        """Service log level."""
        return self.__logging_level

    @logging_level.setter
    def logging_level(self, level: Union[int, str]) -> None:
        if level in ("D", "DBG", "DEBUG", logging.DEBUG):
            self.__logging_level = logging.DEBUG
        elif level in ("I", "INF", "INFO", "INFORMATION", logging.INFO):
            self.__logging_level = logging.INFO
        elif level in ("W", "WRN", "WARN", "WARNING", logging.WARNING):
            self.__logging_level = logging.WARNING
        elif level in ("E", "ERR", "ERROR", logging.ERROR):
            self.__logging_level = logging.ERROR
        elif level in ("C", "CRT", "CRIT", "CRITICAL", logging.CRITICAL):
            self.__logging_level = logging.CRITICAL
        elif level in ("F", "FTL", "FAT", "FATAL", logging.FATAL):
            self.__logging_level = logging.FATAL
        else:
            self.__logging_level = logging.DEBUG
        self.logger.setLevel(self.__logging_level)
        for handler in self.logger.handlers:
            handler.setLevel(self.__logging_level)
        self.logger.debug(
            "Logging level set to %s", logging.getLevelName(self.logging_level)
        )

    def process_messages(self) -> None:
        """Function to process messages."""

    def process_tasks(self) -> None:
        """Function to process task queue."""

    def do_job(self) -> None:
        """Job function, which is executed each cycle of the service main loop."""

    def start(self):
        """Starts the main loop of the service."""
        self.initialize()
        try:
            while True:
                ms = time_ms()
                self.process_messages()
                if self._exit:
                    break
                self.process_tasks()
                self.do_job()
                time.sleep(self.delay)
                self.last_loop_timestamp_ms = ms
            self.stop()
        except KeyboardInterrupt:
            self.logger.warning("Keyboard interrupt (SIGINT) received...")
            self.stop()

    def initialize(self):
        """Any initialization before starting the main loop is done here."""

    def cleanup(self):
        """Cleanup function before exit."""

    def stop(self):
        """Service stop function."""
        self.logger.info("Cleaning up...")
        self.cleanup()
        sys.exit(0)

    def _handle_sigterm(self, sig, frame):
        """SIGTERM handling function."""
        self.logger.warning("SIGTERM received... sig:%s frame:%s", sig, frame)
        self.stop()
