"""
Provides service class which use Redis server for data storage and pub/sub communication.
"""

from typing import Union
import sys
from datetime import datetime, timezone
import logging
import redis

from .simple_service import SimpleService
from .__helpers import time_ms


class RedisStreamHandler(logging.Handler):
    """Stream handler to send logs to Redis"""

    def __init__(
        self,
        worker_name: str,
        connection_conf: Union[dict, None] = None,
        stream_name: str = "worker_logs",
    ) -> None:
        super().__init__()
        self.worker_name: str = worker_name
        if connection_conf is None:
            connection_conf = {"host": "localhost", "port": 6379, "db": 0}
        self.redis_client: redis.client.Redis = redis.StrictRedis(
            **connection_conf, decode_responses=True
        )
        self.stream_name: str = stream_name

    def emit(self, record):
        """Emit log record to redis stream"""
        # pylint: disable=broad-exception-caught
        try:
            # Create log entry as a dictionary with the required fields
            log_entry = {
                "worker_name": self.worker_name,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "log_level": record.levelname,
                "log_message": record.getMessage(),
            }

            # Add the log entry to the Redis stream
            self.redis_client.xadd(self.stream_name, log_entry)

        except redis.exceptions.ConnectionError as conn_err:
            print(f"ConnectionError while logging to Redis: {conn_err}")

        except redis.exceptions.DataError as data_err:
            print(f"DataError while logging to Redis: {data_err}")

        except Exception as e:
            # Log any other exception types not anticipated but avoid masking the error completely
            print(f"Unexpected error while logging to Redis: {e}")


class RedisLogFormatter(logging.Formatter):
    """
    Custom formatter to include worker name, timestamp (UTC), log level, and log message

    def format(self, record):
        return super().format(record)

    """


class RedisService(SimpleService):
    """Service class with Redis data storage and pub/sub capabilities."""

    # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        service_name: str = "service",
        version: str = "0.0.1",
        delay: float = 5,
        **kwargs,
    ) -> None:
        self.__redis_conf = {
            "host": kwargs.get("redis_host", "localhost"),
            "port": kwargs.get("redis_port", 6379),
            "db": kwargs.get("redis_db", 0),
        }
        super().__init__(
            service_name=service_name, version=version, delay=delay, **kwargs
        )
        self.redis_cli: redis.client.Redis = redis.Redis(**self.__redis_conf)
        self.ts = self.redis_cli.ts()
        self.__ts_labels: list[str] = []
        self._get_ts_labels()
        self.pubsub = self.redis_cli.pubsub()
        self.pubsub.subscribe(self.service_name)

        self.redis_cli.hset(self.service_name, "version", value=self.version)
        self.redis_cli.hset(self.service_name, "delay", value=str(self.delay))
        self.redis_cli.hset(
            self.service_name, "logging_level", value=str(self.logging_level)
        )
        self.redis_cli.hset(self.service_name, "running", value="0")

    def _logger_add_custom_handler(self) -> None:
        """Override this method to add custom handler to logger"""
        # Create Redis stream handler
        redis_handler = RedisStreamHandler(
            worker_name=self.service_name,
            connection_conf=self.__redis_conf,
            stream_name="worker_logs",
        )

        # Assign custom formatter to Redis handler
        formatter = RedisLogFormatter()
        redis_handler.setFormatter(formatter)

        # Add the handler to the logger
        self._logger.addHandler(redis_handler)

    @property
    def ts_labels(self) -> list[str]:
        """list of time series labels available"""
        return self.__ts_labels

    def _get_ts_labels(self) -> None:
        """populates list of time series labels, fixes aggregation rules"""
        self.__ts_labels = self.ts.queryindex([f"name={self.service_name}", "type=src"])

    def parse_message(self, msg: dict) -> tuple[str, list[str]]:
        """
        Parses message received on Redis pub/sub channel to extract command and parameters list.
        :param msg: Message dict for parsing.
        :return: command string and a list of parameters.
        """
        cmd: str = ""
        params: list[str] = []
        if msg["type"] == "message":
            self.logger.debug("Got message: %s", msg)
            params = msg["data"].decode("utf-8").strip().split("::")
            try:
                cmd = params.pop(0)
            except IndexError:
                params = []
            self.logger.debug("CMD: %s", cmd)
            self.logger.debug("PAR: %s", params)
        return cmd, params

    def process_messages(self):
        while True:
            msg = self.pubsub.get_message(ignore_subscribe_messages=True)
            if msg is None:
                break
            if msg["channel"].decode("utf-8") != self.service_name:
                continue
            if msg["type"] != "message":
                continue
            cmd, params = self.parse_message(msg)
            if cmd == "exit":
                self._exit = True
            elif cmd == "delay" and len(params) > 0:
                try:
                    self.delay = float(params[0])
                except (TypeError, ValueError, IndexError):
                    self.logger.warning("Wrong argument for delay received")
            else:
                if not self.execute_cmd(cmd, params):
                    self.logger.warning("Command %s can not be executed", cmd)

    def execute_cmd(self, cmd: str, params: list[str]):
        """
        Execute command received.
        :param cmd: Command to be executed.
        :param params: list of parameters for the command.
        :return: True if command execution was successful otherwise false.
        """
        self.logger.debug("CMD: %s, PAR: %s", cmd, params)
        return False

    def initialize(self):
        self.redis_cli.hset(self.service_name, "running", value="1")

    def stop(self):
        self.redis_cli.hset(self.service_name, "running", value="0")
        self.logger.info("Cleaning up...")
        self.cleanup()
        self.redis_cli.close()
        sys.exit(0)

    def create_time_series_channel(
        self,
        label: str,
        retention: int = 2_592_000,  # 30 days
        aggregation: Union[tuple[int], list[int], None] = None,
    ):
        """
        Creates time series channel with the given label and retention time.
        If aggregation times are provided as an iterable of numeric values in seconds
        method also creates average and standard deviation aggregation channels
        and sets the rules for them in redis.
        Aggregation channels are named using the following pattern:
            label_avg_60s or label_std.s_30s
        where label is the channel's label and the last part of the name is the aggregation time.

        :param label: The label of the channel.
        :param retention: Retention time in seconds, defaults to 30 days.
        :param aggregation: An optional iterable of aggregation time values in seconds.
        """
        retention_ms = int(max(0, retention * 1000))
        try:
            self.ts.create(
                label,
                retention_msecs=retention_ms,
                labels={"name": self.service_name, "type": "src"},
            )
        except redis.exceptions.ResponseError:
            pass
        self._get_ts_labels()
        if isinstance(aggregation, (list, tuple)):
            for aggr_t in aggregation:
                self.add_time_series_aggregation(label, aggr_t, retention)

    def del_time_series_channel(self, label: str) -> None:
        """Delete time series and all its aggregations by label"""
        if label in self.__ts_labels:
            ts_info = self.ts.info(label)
            for rule in ts_info.rules:
                self.redis_cli.delete(rule[0].decode("utf-8"))
            self.redis_cli.delete(label)
            self._get_ts_labels()

    def add_time_series_aggregation(
        self,
        label: str,
        aggregation: int = 10,  # seconds
        retention: int = 2_592_000,  # seconds, default value is 30 days
    ) -> None:
        """
        Creates average and standard deviation aggregation channels for given channel label
        and sets the rules for them in redis.
        Aggregation channels are named using the following pattern:
            label_avg_60s or label_std_30s
        where label is the channel label and the last part of the name is the aggregation time.

        :param label: The label of the channel.
        :param aggregation: Aggregation time in seconds.
        :param retention: Retention time in seconds, defaults to 30 days.
        """
        if label in self.__ts_labels:
            retention_ms = int(max(0, retention)) * 1000
            aggregation = int(max(0, aggregation))
            aggr_retention_ms = retention_ms * max(1, aggregation)
            aggregation_ms = int(max(0, aggregation)) * 1000
            try:
                for fun in ("avg", "std.s"):
                    self.ts.create(
                        f"{label}_{fun}_{aggregation}s",
                        retention_msecs=aggr_retention_ms,
                        labels={"name": self.service_name, "type": fun},
                    )
                    # Create averaging rule
                    self.ts.createrule(
                        label,
                        f"{label}_{fun}_{aggregation}s",
                        fun,
                        bucket_size_msec=aggregation_ms,
                    )
            except redis.exceptions.ResponseError:
                pass

    def del_time_series_aggregation(self, label: str, aggr_t: int) -> None:
        """Delete time series aggregation"""
        if label in self.__ts_labels:
            for fun in ("avg", "std.s"):
                aggr_label = f"{label}_{fun}_{aggr_t}s"
                self.redis_cli.delete(aggr_label)

    def put_ts_data(
        self, label: str, value: float, timestamp_ms: Union[int, None] = None
    ):
        """Puts data to redis time series channel"""
        if timestamp_ms is None:
            timestamp_ms = time_ms()
        self.ts.add(
            label,
            timestamp_ms,
            value,
            labels={"name": self.service_name, "type": "src"},
        )
