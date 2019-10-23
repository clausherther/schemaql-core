import logging
from colorama import init
from colorama import Fore, Back, Style
init(autoreset=True)


def make_logger():
    """
    Return a logger instance.
    """
    _logger = logging.getLogger(__name__)

    # message_format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    message_format = "%(asctime)s | %(message)s"
    formatter = logging.Formatter(message_format, datefmt="%H:%M:%S")
    # '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    fh = logging.FileHandler('logs/schemaql.log')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    _logger.addHandler(ch)
    _logger.addHandler(fh)
    _logger.setLevel(logging.INFO)

    return _logger


logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)
logger = make_logger()
