import logging
import os
from datetime import datetime
from pytz import timezone

ET = timezone('America/New_York')


class ETFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, ET)
        return dt.strftime('%Y-%m-%d %H:%M:%S ET')


def setup_logger(name: str) -> logging.Logger:
    os.makedirs('logs', exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    date_str = datetime.now(ET).strftime('%Y%m%d')
    fh = logging.FileHandler(f'logs/{name}_{date_str}.log')
    fh.setFormatter(ETFormatter('%(asctime)s | %(levelname)s | %(message)s'))
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(ETFormatter('%(asctime)s | %(levelname)s | %(message)s'))
    logger.addHandler(ch)

    return logger
