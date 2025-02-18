import logging
import time


# Adjust this import if your Logger class is in another module file.
from Make_It_Parquet.user_interface.logger import Logger


def test_logger_creation_valid_level():
    """Test that creating a Logger with a valid log level sets up everything correctly."""
    test_logger = Logger("DEBUG")

    # Check that active_log_level is set correctly.
    assert test_logger.active_log_level == logging.DEBUG
    # Check that the underlying logger's level is set.
    assert test_logger.logger.level == logging.DEBUG

    # Check that the helper components are created.
    assert test_logger.console_handler is not None
    assert test_logger.queue_handler is not None
    assert test_logger.queue_listener is not None

    # Check that the queue handler is attached to the logger.
    assert test_logger.queue_handler in test_logger.logger.handlers

    # Verify that the console handler uses the correct formatter.
    expected_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    assert test_logger.console_handler.formatter._fmt == expected_fmt


def test_logger_creation_invalid_level():
    """Test that an invalid log level defaults to INFO."""
    test_logger = Logger("INVALID")

    # Should default to INFO.
    assert test_logger.active_log_level == logging.INFO
    assert test_logger.logger.level == logging.INFO


def test_stop_logging_and_queue_empty():
    """Test that stop_logging processes all queued log records and empties the queue."""
    test_logger = Logger("INFO")

    # Log some messages.
    test_logger.logger.info("Test message 1")
    test_logger.logger.warning("Test message 2")

    # Allow a brief moment for the asynchronous logging to enqueue the messages.
    time.sleep(0.1)

    # Stop logging.
    test_logger.stop_logging()

    # Optionally wait a moment to ensure the listener finished processing.
    time.sleep(0.1)

    # After stopping, the log queue should be empty.
    assert test_logger.log_queue.empty()


def test_stop_logging_multiple_calls():
    """Ensure calling stop_logging more than once doesn't raise an exception."""
    test_logger = Logger("DEBUG")
    test_logger.logger.info("Another test message")

    # Call stop_logging twice.
    test_logger.stop_logging()
    # Second call should not raise an exception.
    test_logger.stop_logging()
