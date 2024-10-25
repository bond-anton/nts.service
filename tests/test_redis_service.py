"""Unittests for RedisService class."""

import unittest
import threading
import time
import redis


try:
    from src.nts.service import RedisService, LogLevel
except ModuleNotFoundError:
    from nts.service import RedisService, LogLevel


class TestRedisService(unittest.TestCase):
    """
    Test SimpleService class.
    """

    class Worker(RedisService):
        # pylint: disable=too-many-instance-attributes
        """RedisService worker implementation"""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.count = 0
            self.max_count = 12

        def do_job(self) -> None:
            self.count += 1
            if self.count > self.max_count - 1:
                self._exit = True

    def test_create_delete_time_series_channel(self):
        """Test time_series_channel creation"""
        try:
            worker = self.Worker(
                service_name="TestRedisWorker",
                version="1.0.1",
                delay=0.1,
                logging_level=LogLevel.DEBUG,
            )
            # test service name is correctly set
            ch_name = "test_ch_2345ghdkuuu"
            worker.del_time_series_channel(ch_name)
            self.assertEqual(worker.ts_labels, [])
            worker.create_time_series_channel(
                ch_name, retention=2000, aggregation=(1, 60)
            )
            self.assertEqual(worker.ts_labels, [ch_name])
            self.assertEqual(len(worker.ts.info(ch_name).rules), 4)
            worker.del_time_series_aggregation(ch_name, 60)
            self.assertEqual(len(worker.ts.info(ch_name).rules), 2)
            worker.put_ts_data(ch_name, 3.14)
            worker.del_time_series_channel(ch_name)
            self.assertEqual(worker.ts_labels, [])

            redis_cli: redis.Redis = redis.Redis(host="localhost", port=6379)
            redis_cli.delete("TestRedisWorker")
        except redis.exceptions.ResponseError:
            pass

    def test_redis_service(self) -> None:
        """
        Make an implementation of RedisService and test that it starts and stops on redis cmd.
        """
        try:
            worker1 = self.Worker(
                service_name="TestRedisWorker",
                version="1.0.1",
                delay=1,
                logging_level=LogLevel.DEBUG,
            )

            def publish_stop_signal():
                redis_cli: redis.Redis = redis.Redis(host="localhost", port=6379)
                # You could do something more robust to wait until worker is loaded
                time.sleep(0.1)
                redis_cli.publish("TestRedisWorker", "my_command")
                time.sleep(0.1)
                redis_cli.publish("TestRedisWorker", " ")
                time.sleep(1)
                redis_cli.publish("TestRedisWorker", "delay::1.2")
                time.sleep(1)
                redis_cli.publish("TestRedisWorker", "delay::aaa")
                time.sleep(1)
                redis_cli.publish("TestRedisWorker", "exit")

            thread1 = threading.Thread(target=publish_stop_signal)
            thread1.daemon = True
            thread1.start()

            with self.assertRaises(SystemExit) as cm:
                worker1.start()
            self.assertEqual(cm.exception.code, 0)
            self.assertTrue(worker1.count < worker1.max_count)
            self.assertTrue(worker1.count > 0)

            redis_cli: redis.Redis = redis.Redis(host="localhost", port=6379)
            redis_cli.delete("TestRedisWorker")
        except redis.exceptions.ResponseError:
            pass


if __name__ == "__main__":
    unittest.main()
