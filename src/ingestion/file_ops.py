import os
import pandas as pd
from datetime import datetime
from src.utils.logger import logger
from src.utils.config_loader import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.db.db_ops import Clients
from collections import Counter
from src.db.db_ops import ProcessingJob
from src.utils.utils import find_id_column, is_valid_israeli_id
import shutil
import logging
import re


def ensure_filename_suffix(file_path):
    filename = os.path.basename(file_path)
    if '_' not in filename or not filename.split('_')[-1].split('.')[0].replace('-', '').isdigit():
        suffix = datetime.now().strftime('_%m%Y')
        new_filename = filename.split('.')[0] + suffix + '.' + filename.split('.')[-1]
        new_path = os.path.join(os.path.dirname(file_path), new_filename)
        os.rename(file_path, new_path)
        return new_path
    return file_path

def rename_file_with_id_column(file_path, session, job_id):
    try:
        filename = os.path.basename(file_path)
        base, ext = os.path.splitext(filename)
        suffix = datetime.now().strftime('_%m%Y')
        new_filename = f"{base}{suffix}{ext}"
        new_path = os.path.join(os.path.dirname(file_path), new_filename)
        os.rename(file_path, new_path)
        logger.info(f"Renamed {file_path} to {new_path}", extra={"job_id": job_id})
        return new_path, None
    except Exception as e:
        logger.error(f"Error renaming {file_path}", extra={"job_id": job_id, "error": str(e)})
        new_path = move_file(file_path, 'data/failed', suffix='failed')
        session.query(ProcessingJob).filter_by(job_id=job_id).update({
            'status': 'FAILED',
            'error_summary': str(e),
            'finished_at': datetime.utcnow()
        })
        session.commit()
        return None, None

def move_file(file_name, dest_dir, job_id, status, report_period):
    """Move and rename file to original_name_reported_date_job_id_status.extension."""
    # Extract original name and extension from full path
    base_name = os.path.basename(file_name)
    original_name, ext = os.path.splitext(base_name)
    
    # Remove existing job_id_status if resubmission
    pattern = r'_\d+_(STARTED|SUCCESS|FAILED|PARTIAL)$'
    if re.search(pattern, original_name):
        original_name = re.sub(pattern, '', original_name)
    
    # Construct new file name
    reported_date = report_period.replace('-', '')  # e.g., '09-2025' -> '092025'
    new_name = f"{original_name}_{reported_date}_{job_id}_{status}{ext}"
    dest_path = os.path.join(dest_dir, new_name)
    
    # Ensure dest_dir exists
    os.makedirs(dest_dir, exist_ok=True)
    
    # Move/rename file using full source path
    try:
        os.rename(file_name, dest_path)  # Use full file_name, not base_name
        logger.info(f"Moved file to {dest_path}", extra={"job_id": job_id})
        return dest_path
    except Exception as e:
        logger.error(f"Failed to move file {file_name} to {dest_path}: {str(e)}", extra={"job_id": job_id})
        return None

def handle_fee_outliers(result):
    """Handle outlier saving and return updated df and status."""
    logger = logging.getLogger(__name__)
    df = result['df']
    outliers = result['outliers']
    outlier_count = result['outlier_count']
    max_outliers = result['max_outliers']
    job_id = result['job_id']
    file_path = result['file_path']
    provider = result['provider']
    
    # Construct dynamic filename
    filename = os.path.basename(file_path)
    base, _ = os.path.splitext(filename)
    base_filename = f"{base}_{provider}_{datetime.now().strftime('%m%Y')}"
    
    # Archive original file
    archive_dir = 'data/archive'
    os.makedirs(archive_dir, exist_ok=True)
    archive_path = os.path.join(archive_dir, f"{base}_archive_{datetime.now().strftime('%m%Y')}.xlsx")
    try:
        shutil.copy(file_path, archive_path)
        logger.info(f"Archived {file_path} to {archive_path}", extra={"job_id": job_id})
    except Exception as e:
        logger.error(f"Failed to archive {file_path}: {str(e)}", extra={"job_id": job_id})
    
    # Handle outliers
    if outlier_count > max_outliers:
        failed_file = f"data/failed/{base_filename}_failed.xlsx"
        os.makedirs('data/failed', exist_ok=True)
        df.to_excel(failed_file, index=False)
        logger.error(f"Too many outliers ({outlier_count}), saved to {failed_file}", extra={"job_id": job_id})
        return {'status': 'failed', 'df': df}
    
    if outlier_count > 0:
        problematic_rows = outliers.copy()
        success_file = f"data/processed/{base_filename}_1_success.xlsx"
        attention_file = f"data/staging/{base_filename}_2_attention.xlsx"
        os.makedirs('data/processed', exist_ok=True)
        os.makedirs('data/staging', exist_ok=True)
        df.to_excel(success_file, index=False)
        problematic_rows.to_excel(attention_file, index=False)
        logger.info(f"Saved {len(df)} rows to {success_file}, {outlier_count} outliers to {attention_file}", extra={"job_id": job_id})
    else:
        success_file = f"data/processed/{base_filename}_success.xlsx"
        os.makedirs('data/processed', exist_ok=True)
        df.to_excel(success_file, index=False)
        logger.info(f"Saved {len(df)} rows to {success_file}, no outliers", extra={"job_id": job_id})
    
    return {'status': 'ok', 'df': df}