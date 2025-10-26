import datetime
import logging
import os

from dotenv import load_dotenv

# Load from .env if it exists (useful for local development)
load_dotenv()


default_level = logging.WARNING
logging.basicConfig(
    level=default_level,
    format="%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d - %(funcName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("spotify_playlist_generator")

level_to_set = os.getenv("LOGGING_LEVEL", "").upper()
valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if level_to_set in valid_levels:
    logger.warning(f"Logger level set to: {level_to_set}")
    logging.basicConfig(
        level=level_to_set,
        format="%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d - %(funcName)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.warning(f"Logger level set to: {level_to_set}")
else:
    logger.warning(
        f"Invalid logging level: {level_to_set}. Level not changed.\n"
        f"default_level: {default_level}"
    )


# Shortcut aliases
debug = logger.debug
info = logger.info
warning = logger.warning
error = logger.error
exception = logger.exception


def get_logger():
    return logger


def format_date(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")
