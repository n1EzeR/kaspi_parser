import logging
import os
import time
from datetime import date
from typing import Callable

from .exceptions import EmptyDirectoryException

LOGGER = logging.getLogger()


def perf_logger(func: Callable):
    def wrapper(*args, **kwargs):
        LOGGER.info(f"Started {func.__name__}")
        start = time.perf_counter()
        func(*args, **kwargs)
        LOGGER.info(f"Finshed {func.__name__} in {time.perf_counter() - start}")

    return wrapper


def parse_latest_date(dir):
    parse_date = date.today()

    if not os.path.exists(f"{dir}/{parse_date}"):
        LOGGER.warning(f"Products for {parse_date} are not parsed and collected yet")
        parse_date = _get_latest_date_in_dir(dir)

    LOGGER.info(f"Reviews from {parse_date} are being parsed")

    return parse_date


def _get_latest_date_in_dir(dir):
    parse_dates = sorted(os.listdir(dir))
    parse_dates = [
        parse_date for parse_date in parse_dates if not parse_date.startswith(".")
    ]

    if not parse_dates:
        raise EmptyDirectoryException(
            f"No valid directories for parsing are found in {dir}"
        )

    return parse_dates[-1]
