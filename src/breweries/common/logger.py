import logging
import sys


APP_LOGGER_NAME = "EMR_ABI_INBEV_SPARK"


def setup() -> None:
    log_level = logging.INFO

    logger = logging.getLogger(APP_LOGGER_NAME)
    logger.setLevel(log_level)

    # Add logging to CONSOLE
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(console_handler)


def get() -> logging.Logger:
    """Gets a logger instance properly configured

    Returns:
        A logger with the correct handlers and formats

    """
    return logging.getLogger(APP_LOGGER_NAME)
