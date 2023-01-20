"""
Some utility functions.
"""
import logging
import os
from typing import Callable


def run_with_log(filename: str) -> Callable:
    """
    Decorator for running a procedure and saving the log to a .log file.

    Parameters
    ----------
    filename : str
        Name of .log file.

    """

    dir_name, _ = os.path.split(__file__)
    filename = os.path.join(dir_name, f"{filename}.log")
    logging.basicConfig(level=logging.INFO,
                        filename=filename,
                        filemode="a",
                        format="%(asctime)s %(levelname)-8s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    def decorator(function: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            try:
                logging.info("Started")
                function(*args, **kwargs)
                logging.info("Finished")
            except Exception as ex:
                logging.exception(ex)
                raise ex

        return wrapper

    return decorator
