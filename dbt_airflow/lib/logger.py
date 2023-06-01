import logging
from os import environ

import google.cloud.logging

LOGGER = None


def _config_logger():

    if environ.get("USE_CLOUD_LOGGER") == "True":
        client = google.cloud.logging.Client()
        client.setup_logging()

    logger = logging.getLogger()
    return logger


def get_logger():
    global LOGGER
    if LOGGER is None:
        LOGGER = _config_logger()
    return LOGGER
