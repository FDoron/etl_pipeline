import re
import os
# from src.utils.logger import get_logger
import shutil
from src.utils.logger import logger

# logger = get_logger(__name__)

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

def move_file(file_path: str, target_dir: str, job_id: int) -> bool:
    """Move file to target directory, log with job_id."""
    try:
        target_path = os.path.join(target_dir, os.path.basename(file_path))
        os.makedirs(target_dir, exist_ok=True)
        shutil.move(file_path, target_path)
        logger.info(f"Moved {file_path} to {target_path}", extra={"job_id": job_id})
        return True
    except Exception as e:
        logger.error(f"Failed to move {file_path} to {target_dir}: {str(e)}", extra={"job_id": job_id})
        return False


