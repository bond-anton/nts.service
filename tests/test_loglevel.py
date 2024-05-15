"""Testing LogLevel enum"""

import unittest
import logging

try:
    from src.nts.service import LogLevel
except ModuleNotFoundError:
    from nts.service import LogLevel


class TestLogLevel(unittest.TestCase):
    """
    Test LogLevel Enum.
    """

    def test_loglevel_values(self) -> None:
        """
        test values of LogLevel Enum.
        """
        self.assertEqual(len(LogLevel), 4)
        self.assertEqual(LogLevel.DEBUG, "DEBUG")
        self.assertEqual(LogLevel.INFO, "INFO")
        self.assertEqual(LogLevel.WARN, "WARN")
        self.assertEqual(LogLevel.ERROR, "ERROR")

        self.assertIsInstance(logging.getLevelName(LogLevel.DEBUG), int)
        self.assertIsInstance(logging.getLevelName(LogLevel.INFO), int)
        self.assertIsInstance(logging.getLevelName(LogLevel.WARN), int)
        self.assertIsInstance(logging.getLevelName(LogLevel.ERROR), int)

        self.assertEqual(
            logging.getLevelName(LogLevel.DEBUG), logging.getLevelName("DEBUG")
        )
        self.assertEqual(
            logging.getLevelName(LogLevel.INFO), logging.getLevelName("INFO")
        )
        self.assertEqual(
            logging.getLevelName(LogLevel.WARN), logging.getLevelName("WARN")
        )
        self.assertEqual(
            logging.getLevelName(LogLevel.ERROR), logging.getLevelName("ERROR")
        )


if __name__ == "__main__":
    unittest.main()
