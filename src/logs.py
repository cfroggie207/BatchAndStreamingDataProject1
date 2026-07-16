import sys
import logging
import logging.config

log = logging.getLogger('')

def setup_logging():
   """Sets up the logging configuration for the logger"""
   CONFIG = {
      'version': 1,
      'formatters': {
         # Modify log message format here or replace with your custom formatter class
         'my_formatter': {
            'format': '[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] | %(message)s',
            'datefmt': '%a, %d %b %Y %H:%M:%S'
         }
      },
      'handlers': {
         'console_stderr': {
            # Sends log messages with log level ERROR or higher to stderr
            'class': 'logging.StreamHandler',
            'level': 'ERROR',
            'formatter': 'my_formatter',
            'stream': sys.stderr
         },
         'console_stdout': {
            # Sends log messages with log level lower than ERROR to stdout
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'my_formatter',
            'stream': sys.stdout
         },
      },
      'root': {
         # In general, this should be kept at 'NOTSET'.
         # Otherwise it would interfere with the log levels set for each handler.
         'level': 'NOTSET',
         'handlers': ['console_stderr', 'console_stdout']
      },
   }

   logging.config.dictConfig(CONFIG)

def get_logger():
   return logging.getLogger('')
