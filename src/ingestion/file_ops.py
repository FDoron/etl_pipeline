import os
from datetime import datetime
from src.utils.logger import logger

# def ensure_filename_suffix(file_path):
#     """Ensure file has _YYYY-MM suffix based on current date if missing."""
#     base, ext = os.path.splitext(file_path)
#     if not base.endswith('_' + datetime.now().strftime('%Y-%m')):
#         new_base = f"{base}_{datetime.now().strftime('%Y-%m')}"
#         new_path = f"{new_base}{ext}"
#         try:
#             os.rename(file_path, new_path)
#             logger.info("Renamed file with date suffix", extra={"original": file_path, "new": new_path})
#             return new_path
#         except OSError as e:
#             logger.error("Failed to rename file", extra={"file": file_path, "error": str(e)})
#             raise
#     return file_path

def ensure_filename_suffix(file_path):
    filename = os.path.basename(file_path)
    if '_' not in filename or not filename.split('_')[-1].split('.')[0].replace('-', '').isdigit():
        suffix = datetime.now().strftime('_%m%Y')
        new_filename = filename.split('.')[0] + suffix + '.' + filename.split('.')[-1]
        new_path = os.path.join(os.path.dirname(file_path), new_filename)
        os.rename(file_path, new_path)
        return new_path
    return file_path

def move_file(file_path, target_dir, suffix=None):
    """Move file to target directory, avoiding duplicates by adding timestamp."""
    os.makedirs(target_dir, exist_ok=True)
    base, ext = os.path.splitext(os.path.basename(file_path))
    if suffix:
        new_name = f"{base}_{suffix}{ext}"
    else:
        new_name = os.path.basename(file_path)
    target_path = os.path.join(target_dir, new_name)
    
    # Handle duplicates by appending timestamp
    counter = 1
    while os.path.exists(target_path):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        new_name = f"{base}_{suffix}_{timestamp}_{counter}{ext}" if suffix else f"{base}_{timestamp}_{counter}{ext}"
        target_path = os.path.join(target_dir, new_name)
        counter += 1
    
    try:
        os.rename(file_path, target_path)
        logger.info("Moved file", extra={"original": file_path, "target": target_path})
        return target_path
    except OSError as e:
        logger.error("Failed to move file", extra={"file": file_path, "target": target_path, "error": str(e)})
        raise