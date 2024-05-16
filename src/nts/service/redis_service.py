"""
Provides service class which use Redis server for data storage and pub/sub communication.
"""

from typing import Union
import sys
import redis

from .simple_service import SimpleService
from .__helpers import time_ms


class RedisService(SimpleService):
    """Service class with Redis data storage and pub/sub capabilities."""

    def __init__(
        self,
        service_name: str = "Redis Service",
        version: str = "0.0.1",
        delay: float = 5,
        username: str = "worker",
        **kwargs,
    ) -> None:
        super().__init__(
            service_name=service_name, version=version, delay=delay, **kwargs
        )
        self.__username: str = str(username)
        self.redis_cli: redis.Redis = redis.Redis(
            host=kwargs.get("redis_host", "localhost"),
            port=kwargs.get("redis_port", 6379),
        )
        self.ts = self.redis_cli.ts()
        self.__ts_labels: list[str] = []
        self._get_ts_labels()
        self.pubsub = self.redis_cli.pubsub()
        self.pubsub.subscribe(self.username)

        self.redis_cli.hset(self.service_name, "username", value=self.username)
        self.redis_cli.hset(self.service_name, "version", value=self.version)
        self.redis_cli.hset(self.service_name, "delay", value=str(self.delay))
        self.redis_cli.hset(
            self.service_name, "logging_level", value=str(self.logging_level)
        )
        self.redis_cli.hset(self.service_name, "running", value="0")

    @property
    def username(self) -> str:
        """
        Service username property is used as a dedicated channel name
        for the Redis pub/sub communication.
        """
        return self.__username

    @property
    def ts_labels(self) -> list[str]:
        """list of time series labels available"""
        return self.__ts_labels

    def _get_ts_labels(self) -> None:
        """populates list of time series labels, fixes aggregation rules"""
        self.__ts_labels = self.ts.queryindex([f"name={self.username}", "type=src"])

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
            if msg["channel"].decode("utf-8") != self.username:
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
                labels={"name": self.username, "type": "src"},
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
                        labels={"name": self.username, "type": fun},
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
            labels={"name": self.username, "type": "src"},
        )
