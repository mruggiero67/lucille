"""Standard logging configuration for lucille scripts.

Prior to this module, ~30 scripts each called ``logging.basicConfig(...)`` at
import time with slightly different formats. This module offers one function
so the drift stops.
"""

import logging

DEFAULT_FORMAT = "%(levelname)-8s %(asctime)s %(filename)s:%(lineno)d %(message)s"
DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"


def setup_logging(verbose: bool = False) -> None:
    """Configure the root logger with lucille's standard format.

    Args:
        verbose: If True, log at DEBUG level; otherwise INFO.
    """
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=DEFAULT_FORMAT,
        datefmt=DEFAULT_DATEFMT,
    )
