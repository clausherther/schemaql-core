import logging

def make_logger():
    """
    Return a logger instance.
    """
    _logger = logging.getLogger()

    # message_format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    message_format = '%(message)s'
    formatter = logging.Formatter(message_format)
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


logger = make_logger()