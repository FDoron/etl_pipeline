# src/utils/logger.py
import logging
from pathlib import Path

def get_logger(name: str):
    # Define log file path
    log_file = Path("logs/pipeline.log")
    
    # Ensure 'logs/' directory exists
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid adding multiple handlers if logger already exists
    if not logger.handlers:
        # File handler
        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # Optional: console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger
