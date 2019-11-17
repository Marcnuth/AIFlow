import logging
from logging.config import dictConfig
import os


dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "basic": {
            "format": "[%(asctime)s] [%(process)d:%(thread)d] [%(levelname)s] [%(name)s] %(filename)s:%(funcName)s:%(lineno)d %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "level": "WARN",
            "class": "logging.StreamHandler",
            "formatter": "basic",
            "stream": "ext://sys.stdout"
        },
    },
    "loggers": {
        "aiflow": {
            "handlers": ["console"],
            "propagate": "true",
            "level": "DEBUG" if os.getenv('DEBUG_MODE', True) else 'WARN'
        }
    }
})

logger = logging.getLogger(__name__)
logger.debug('aiflow logs will be printed in console')
