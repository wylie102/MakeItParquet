import logging
from logging.handlers import QueueHandler, QueueListener
import queue
import sys


class Logger:
    """
    A logger that can be used to log messages to the console via asynchronous queue based logging.
    """

    def __init__(self, log_level: str):
        self.log_queue = queue.Queue()
        self.logger = logging.getLogger("Make-it-Parquet!")

        self.default_log_level = logging.INFO
        self.supplied_log_level = log_level.strip().upper()
        self.active_log_level = None

        self.console_handler = None
        self.queue_handler = None
        self.queue_listener = None

        self._configure_logging()

    def _configure_logging(self):
        """
        Configure asynchronous logging system.

        Sets up queue-based logging with console output.
        """
        self._set_logging_level()
        self._setup_console_handler()
        self._setup_queue_handler()
        self._setup_queue_listener()
        self.queue_listener.start()

    def _set_logging_level(self):
        """
        Set logging level.
        """
        self.active_log_level = getattr(
            logging, self.supplied_log_level, self.default_log_level
        )
        self.logger.setLevel(self.active_log_level)

    def _setup_console_handler(self) -> logging.StreamHandler:
        """
        Setup console handler.
        """
        self.console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.console_handler.setFormatter(formatter)

    def _setup_queue_handler(self) -> QueueHandler:
        """
        Setup queue handler.
        """
        self.queue_handler = QueueHandler(self.log_queue)
        self.logger.addHandler(self.queue_handler)

    def _setup_queue_listener(self) -> QueueListener:
        """
        Setup queue listener.
        """
        self.queue_listener = QueueListener(self.log_queue, self.console_handler)

    def stop_logging(self):
        """
        Stop the logging system cleanly.

        Ensures logging queue is processed before shutdown.
        """
        self.queue_listener.stop()
