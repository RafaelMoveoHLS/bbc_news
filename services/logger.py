from datetime import datetime
import os
import logging
from logging.handlers import TimedRotatingFileHandler


def get_logger() -> logging.Logger:
    """
    Configures and returns a logger that logs messages to both the console and a file.
    The log file includes a timestamp in the filename.

    Returns:
        Logger: Configured logger instance.
    """

    # Create the log file name with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H")  # Current timestamp
    log_file_name = f"runtime_{timestamp}.log"

    # Configure logging with TimedRotatingFileHandler
    log_file_path = os.path.join('logs', log_file_name)

    # Configure the logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path),  # Log to the file with the timestamp
            logging.StreamHandler()              # Log to the console
        ]
    )

    return logging.getLogger(__name__)