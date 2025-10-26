import datetime
import logging

default_level = logging.WARNING
logging.basicConfig(
    level=default_level,
    format="%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d - %(funcName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("spot")
logging.getLogger().warning(f"Logger level set to: {default_level}")

# Shortcut aliases
debug = logger.debug
info = logger.info
warning = logger.warning
error = logger.error
exception = logger.exception


def set_logger_level(level_to_set):
    normalized_level = level_to_set.upper()
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if normalized_level in valid_levels:
        logger.setLevel(getattr(logging, normalized_level))
        logging.getLogger().warning(f"Logger level set to: {normalized_level}")
    else:
        logging.getLogger().warning(
            f"Invalid logging level: {level_to_set}. Level not changed.\n"
            f"default_level: {default_level}, or DEBUG"
        )
    return logger


def get_logger():
    return logger


def format_date(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")
