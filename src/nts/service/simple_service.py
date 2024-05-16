"""Provides SimpleService worker class for running as a daemon in the background."""

from typing import Union
import signal
import sys
import time
import logging

from .__helpers import time_ms

try:
    import systemd.daemon  # type: ignore
except ModuleNotFoundError:
    pass


class SimpleService:
    """
    SimpleService is a worker daemon compatible with systemd,
    can run in the background, and gracefully finishes on SIGTERM and SIGINT.
    """

    def __init__(
        self,
        service_name: str = "service",
        version: str = "0.0.1",
        delay: float = 5,
        **kwargs,
    ) -> None:
        self.__service_name: str = service_name
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
        self.__logger: logging.Logger = self._init_logger()
        self._exit: bool = False

        signal.signal(signal.SIGTERM, self._handle_sigterm)

        if "systemd.daemon" in sys.modules:
            systemd.daemon.notify("READY=1")

        self.last_loop_timestamp_ms = time_ms()

    def _init_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(self.logging_level)
        stdout_handler = logging.StreamHandler()
        stdout_handler.setLevel(self.logging_level)
        stdout_handler.setFormatter(logging.Formatter("%(levelname)8s | %(message)s"))
        logger.addHandler(stdout_handler)
        return logger

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
        return self.__logger

    @property
    def logging_level(self) -> int:
        """Service log level."""
        return self.__logging_level

    @logging_level.setter
    def logging_level(self, level: Union[int, str]) -> None:
        try:
            if not isinstance(logging.getLevelName(level), int):
                self.__logging_level = logging.DEBUG
            else:
                self.__logging_level = logging.getLevelName(level)
        except (TypeError, ValueError):
            pass
        self.logger.setLevel(self.__logging_level)
        for handler in self.logger.handlers:
            handler.setLevel(self.__logging_level)

    def process_messages(self) -> None:
        """Function to process messages received."""

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
