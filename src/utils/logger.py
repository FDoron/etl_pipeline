import logging
import logging.config
import yaml
from pythonjsonlogger import jsonlogger

def setup_logger():
    with open('config/logging.yaml', 'r') as f:
        config = yaml.safe_load(f)
    logging.config.dictConfig(config)
    return logging.getLogger('etl_pipeline')

logger = setup_logger()

def log_action(action, details):
    logger.info(action, extra=details)
    
