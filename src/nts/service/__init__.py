"""This module provides classes for implementation of simple daemons working in a background."""

__version__ = "0.0.1"

from enum import Enum

from .simple_service import SimpleService
from .redis_service import RedisService


class LogLevel(str, Enum):
    """Convenient log levels enumeration."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
