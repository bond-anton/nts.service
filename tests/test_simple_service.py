"""Unittests for SimpleService class."""

import unittest
import os
from signal import SIGINT, SIGTERM
import threading
import time
import logging


try:
    from src.nts.service import SimpleService, LogLevel
except ModuleNotFoundError:
    from nts.service import SimpleService, LogLevel


class TestSimpleService(unittest.TestCase):
    """
    Test SimpleService class.
    """

    class Worker(SimpleService):
        """Example SimpleService worker implementation"""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.count = 0
            self.max_count = 10

        def do_job(self) -> None:
            self.count += 1
            if self.count > self.max_count - 1:
                self._exit = True

    def test_service_name(self) -> None:
        # pylint: disable=attribute-defined-outside-init
        """Test service_name property"""
        worker = self.Worker(
            service_name="TestWorker",
            version="1.0.1",
            delay=0.1,
            logging_level=LogLevel.DEBUG,
        )
        # test service name is correctly set
        self.assertEqual(worker.service_name, "TestWorker")
        # test that service_name is not writable
        with self.assertRaises(AttributeError):
            worker.service_name = "New Name"
        self.assertEqual(worker.service_name, "TestWorker")

    def test_service_version(self) -> None:
        # pylint: disable=attribute-defined-outside-init
        """Test version property"""
        worker = self.Worker(
            service_name="TestWorker",
            version="1.0.1",
            delay=0.1,
            logging_level=LogLevel.DEBUG,
        )
        # test version is correctly set
        self.assertEqual(worker.version, "1.0.1")
        # test that service_name is not writable
        with self.assertRaises(AttributeError):
            worker.version = "2.0.1"
        self.assertEqual(worker.version, "1.0.1")

    def test_delay(self) -> None:
        # pylint: disable=attribute-defined-outside-init
        """Test delay property"""
        worker = self.Worker(
            service_name="TestWorker",
            version="1.0.1",
            delay=0.1,
            logging_level=LogLevel.DEBUG,
        )
        # test correct change of delay value is no problem.
        worker.delay = 0
        self.assertEqual(worker.delay, 0)
        worker.delay = 0.1
        self.assertEqual(worker.delay, 0.1)
        worker.delay = "0.2"
        self.assertEqual(worker.delay, 0.2)
        # test negative delay value assignment is ignored.
        worker.delay = -0.2
        self.assertEqual(worker.delay, 0.2)
        worker.delay = "-0.2"
        self.assertEqual(worker.delay, 0.2)
        # test incorrect value assignment of delay value raises ValueError.
        with self.assertRaises(ValueError):
            worker.delay = "a"
        # test incorrect type assignment of delay value raises TypeError.
        with self.assertRaises(TypeError):
            worker.delay = None
        self.assertEqual(worker.delay, 0.2)

    def test_log_level(self) -> None:
        # pylint: disable=attribute-defined-outside-init
        """Test log_level property"""
        worker = self.Worker(
            service_name="TestWorker",
            version="1.0.1",
            delay=0.1,
            logging_level=LogLevel.DEBUG,
        )
        # test logging level property
        self.assertEqual(worker.logging_level, logging.getLevelName(LogLevel.DEBUG))
        worker.logging_level = LogLevel.INFO
        self.assertEqual(worker.logging_level, logging.getLevelName(LogLevel.INFO))
        worker.logging_level = LogLevel.WARN
        self.assertEqual(worker.logging_level, logging.getLevelName(LogLevel.WARN))
        worker.logging_level = LogLevel.ERROR
        self.assertEqual(worker.logging_level, logging.getLevelName(LogLevel.ERROR))
        worker.logging_level = LogLevel.DEBUG
        self.assertEqual(worker.logging_level, logging.getLevelName(LogLevel.DEBUG))
        worker.logging_level = "test level"
        self.assertEqual(worker.logging_level, logging.getLevelName(LogLevel.DEBUG))
        worker.logging_level = None
        self.assertEqual(worker.logging_level, logging.getLevelName(LogLevel.DEBUG))
        worker.logging_level = [1, 2]
        self.assertEqual(worker.logging_level, logging.getLevelName(LogLevel.DEBUG))
        worker = self.Worker(
            service_name="TestWorker", version="1.0.1", delay=0.1, logging_level=[1, 2]
        )
        self.assertEqual(worker.logging_level, logging.getLevelName(LogLevel.DEBUG))
        worker = self.Worker(
            service_name="TestWorker",
            version="1.0.1",
            delay=0.1,
            logging_level="test level",
        )
        self.assertEqual(worker.logging_level, logging.getLevelName(LogLevel.DEBUG))

    def test_simple_service(self) -> None:
        """
        Make an implementation of SimpleService and test that it starts and stops.
        """
        worker = self.Worker(
            service_name="TestWorker",
            version="1.0.1",
            delay=0.1,
            logging_level=LogLevel.DEBUG,
        )
        with self.assertRaises(SystemExit) as cm:
            worker.start()
        self.assertEqual(cm.exception.code, 0)
        self.assertEqual(worker.count, worker.max_count)

    def test_sig_int(self):
        """Test graceful exit on SIGINT"""
        worker = self.Worker(
            service_name="TestWorker",
            version="1.0.1",
            delay=1,
            logging_level=LogLevel.DEBUG,
        )

        pid = os.getpid()

        def trigger_signal():
            # You could do something more robust, e.g. wait until port is listening
            time.sleep(1)
            os.kill(pid, SIGINT)

        thread = threading.Thread(target=trigger_signal)
        thread.daemon = True
        thread.start()

        with self.assertRaises(SystemExit) as cm:
            worker.start()
        self.assertEqual(cm.exception.code, 0)

    def test_sig_term(self):
        """Test graceful exit on SIGTERM"""
        worker = self.Worker(
            service_name="TestWorker",
            version="1.0.1",
            delay=1,
            logging_level=LogLevel.DEBUG,
        )

        pid = os.getpid()

        def trigger_signal():
            # You could do something more robust, e.g. wait until port is listening
            time.sleep(1)
            os.kill(pid, SIGTERM)

        thread = threading.Thread(target=trigger_signal)
        thread.daemon = True
        thread.start()

        with self.assertRaises(SystemExit) as cm:
            worker.start()
        self.assertEqual(cm.exception.code, 0)


if __name__ == "__main__":
    unittest.main()
