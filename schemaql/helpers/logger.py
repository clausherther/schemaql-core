import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from colorama import init

init(autoreset=True)

DEFAULT_LOG_PATH = Path("logs")


def check_directory_exists(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)


def make_logger():
    """
    Return a logger instance.
    """
    _logger = logging.getLogger(__name__)

    message_format = "%(asctime)s | %(message)s"
    formatter = logging.Formatter(message_format, datefmt="%H:%M:%S")

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    Path(DEFAULT_LOG_PATH).mkdir(parents=True, exist_ok=True)

    log_path = DEFAULT_LOG_PATH.joinpath("schemaql.log")
    fh = TimedRotatingFileHandler(filename=log_path, when="d", interval=1, backupCount=7)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    _logger.addHandler(ch)
    _logger.addHandler(fh)
    _logger.setLevel(logging.INFO)

    _sql_logger = logging.getLogger("sqlalchemy.engine")
    _sql_logger.setLevel(logging.INFO)
    _sql_logger.addHandler(fh)

    return _logger


logger = make_logger()
