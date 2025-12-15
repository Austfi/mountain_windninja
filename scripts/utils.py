import logging
import os
import sys
from datetime import datetime

def setup_logging(name, log_level=logging.INFO):
    """
    Sets up a logger with a consistent format.
    Outputs to console and optionally to a file if configured.
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Check if handlers already exist to avoid duplicate logs
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger

def get_timestamp_str():
    """Returns current UTC timestamp as YYYYMMDD_HHMM."""
    return datetime.utcnow().strftime("%Y%m%d_%H%M")

def ensure_dir(path):
    """Ensures a directory exists."""
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as e:
            # Re-raise if it's not "File exists"
            if not os.path.isdir(path):
                raise
