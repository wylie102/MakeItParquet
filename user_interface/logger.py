import logging
from logging.handlers import QueueHandler, QueueListener
import queue

class Logger:
    def __init__(self, log_level: str):
        self.log_queue = queue.Queue()
        self.logger = logging.getLogger("Make-it-Parquet!")
        self.queue_handler = QueueHandler(self.log_queue)

    def _configure_logging(self):
        """
        Configure asynchronous logging system.

        Sets up queue-based logging with console output.
        """
        self.log_queue = queue.Queue()

        # Create a single logger for the program.
        self.logger = logging.getLogger("Make-it-Parquet!")
        numeric_level = getattr(logging, self.args.log_level.upper(), None)
        if not isinstance(numeric_level, int):
            numeric_level = logging.INFO

        # Console handler with module-aware formatting.
        self.logger.setLevel(numeric_level)
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)

        # Use the queue handler to make logging non-blocking.
        self.queue_handler = QueueHandler(self.log_queue)
        self.logger.addHandler(self.queue_handler)

        # Queue listener processes logs in a background thread.
        self.listener = QueueListener(self.log_queue, console_handler)
        self.listener.start()


    def _stop_logging(self):
        """
        Stop the logging system cleanly.

        Ensures logging queue is processed before shutdown.
        """
        self.listener.stop()
