import logging
import sys
import os
from config import *

def setup_logger(name: str):
    # Create and configure logger
    log_fmt = LOG_FORMAT
    log_file = LOG_FILE
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt,
        force=True
    )
    # Create file handler and stream handler
    file_handler = logging.FileHandler(filename=log_file, mode="a")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_fmt))
    # Remove selenium log
    selenium_logger = logging.getLogger("seleniumwire")
    selenium_logger.setLevel(logging.WARNING)
    # Create logger and add handlers
    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    return logger