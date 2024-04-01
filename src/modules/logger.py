import logging
import sys
from datetime import datetime

from utils import to_string


class LogfmtFormatter(logging.Formatter):
    """
    https://docs.python.org/3/library/logging.html#logrecord-attributes
    """

    def __init__(self, extra=None, *args, **kwargs, ):
        super().__init__(*args, **kwargs)
        self.extra = extra

    def format(self, record) -> str:
        attributes = {
            'asctime': datetime.fromtimestamp(record.created).isoformat(' '),
            'levelname': record.levelname,
            'msg': record.msg.replace('"', '\\"').replace('\n', '\\n'),
        }
        if self.extra:
            attributes.update(self.extra)
        for attribute in attributes:
            if attribute not in ['levelname']:
                attributes[attribute] = f'"{attributes[attribute]}"'
        logfmt = [
            f'{key}={to_string(value)}'
            for key, value in attributes.items()
            if key is not None
        ]
        return ' '.join(logfmt)


def get_logger(default_tags=None):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    if logger.hasHandlers() and default_tags:
        for handler in logger.handlers:
            handler.setFormatter(LogfmtFormatter(extra=default_tags))

    if not logger.hasHandlers():
        handler = logging.StreamHandler(stream=sys.stdout)
        formatter = LogfmtFormatter(extra=default_tags)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

