"""
Provides service class which use Redis server for data storage and pub/sub communication.
"""

import sys
import redis

from .simple_service import SimpleService


class RedisService(SimpleService):
    """Service class with Redis data storage and pub/sub capabilities."""

    def __init__(
        self,
        service_name: str = "Redis Service",
        version: str = "0.0.1",
        delay: float = 5,
        username: str = "worker",
        **kwargs
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
