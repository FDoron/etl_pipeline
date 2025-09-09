import re
from src.utils.logger import get_logger

logger = get_logger(__name__)

def extract_report_period(filename: str):
    """
    Extract 'YYYY-MM' from filename using regex.
    Returns None if no match found.
    """
    match = re.search(r'(\d{4}-\d{2})', filename)
    if match:
        return match.group(1)
    else:
        logger.warning(f"Could not extract report period from filename: {filename}")
        return None
