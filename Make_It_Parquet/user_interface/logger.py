#! /usr/bin/env python3
import logging
import queue
import sys
from logging.handlers import QueueHandler, QueueListener
from typing import TextIO


class Logger(logging.Logger):
    """
    A logger that can be used to log messages to the console via asynchronous queue based logging.
    """

    def __init__(self, log_level: str | None) -> None:
        super().__init__("Make-it-Parquet!")
        self.log_queue: queue.Queue[str] = queue.Queue()

        self.default_log_level: int = logging.INFO
        self.active_log_level: int = self._set_logging_level(log_level)

        self._configure_logging()

    def _set_logging_level(self, log_level: str | None) -> int:
        """Cleans supplied log level and returns verified numeric logging level.
        If None given or value is eroneous returns value of default_log_level."""
        if log_level:
            verified_log_level: str = log_level.strip().upper()
            return getattr(logging, verified_log_level, self.default_log_level)
        else:
            return self.default_log_level

    def _configure_logging(self):
        """
        Configure asynchronous logging system.

        Sets up queue-based logging with console output.
        """
        self.setLevel(self.active_log_level)
        self.console_handler: logging.StreamHandler[TextIO] = (
            self._setup_console_handler()
        )
        self.queue_handler: QueueHandler = self._setup_queue_handler()
        self.queue_listener: QueueListener = self._setup_queue_listener()
        self.queue_listener.start()

    def _setup_console_handler(self):
        """
        Setup console handler.
        """
        console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s - %(message)s", datefmt="%H:%M:%S")
        console_handler.setFormatter(formatter)
        return console_handler

    def _setup_queue_handler(self):
        """
        Setup queue handler.
        """
        queue_handler = QueueHandler(self.log_queue)
        self.addHandler(queue_handler)
        return queue_handler

    def _setup_queue_listener(self):
        """
        Setup queue listener.
        """
        queue_listener = QueueListener(self.log_queue, self.console_handler)
        return queue_listener

    def stop_logging(self):
        """
        Stop the logging system cleanly.

        Ensures logging queue is processed before shutdown.
        """
        self.queue_listener.stop()
