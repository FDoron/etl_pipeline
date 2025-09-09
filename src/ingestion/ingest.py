import pandas as pd
from src.utils.logger import logger
from src.ingestion.file_ops import ensure_filename_suffix, move_file
from src.transformation.transform import normalize_and_validate
from src.db.db_ops import insert_dataframe, ProcessingJob
from datetime import datetime

def ingest_file(file_path, provider_mapping, session, job_id):
    try:
        # Ensure filename has date suffix
        file_path = ensure_filename_suffix(file_path)
        
        # Read CSV
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            logger.error("Failed to read CSV", extra={"file": file_path, "error": str(e)})
            move_file(file_path, 'data/failed', suffix='failed')
            return False
        
        # Normalize and validate
        valid_df, errors = normalize_and_validate(df, provider_mapping, session, job_id)
        
        # Update job status
        rows_processed = len(df)
        rows_failed = len(errors)
        rows_inserted = len(valid_df) if valid_df is not None else 0
        
        session.query(ProcessingJob).filter_by(job_id=job_id).update({
            'rows_processed': rows_processed,
            'rows_failed': rows_failed,
            'rows_inserted': rows_inserted,
            'status': 'PARTIAL' if rows_failed > 0 and rows_inserted > 0 else ('SUCCESS' if rows_inserted > 0 else 'FAILED'),
            'finished_at': datetime.utcnow()
        })
        
        if valid_df is None:
            logger.error("No valid rows after validation", extra={"file": file_path, "errors": errors})
            move_file(file_path, 'data/failed', suffix='failed')
            return False
        
        # Insert valid data to reports table
        success, error = insert_dataframe(session, valid_df, 'reports', job_id)
        if not success:
            logger.error("Failed to insert data to reports", extra={"file": file_path, "error": error})
            move_file(file_path, 'data/failed', suffix='failed')
            return False
        
        # Move file to processed or archive
        target_dir = 'data/archive' if rows_failed > 0 else 'data/processed'
        move_file(file_path, target_dir, suffix='processed' if target_dir == 'data/processed' else 'archived')
        return True
    
    except Exception as e:
        logger.error("Ingestion failed", extra={"file": file_path, "error": str(e)})
        move_file(file_path, 'data/failed', suffix='failed')
        session.query(ProcessingJob).filter_by(job_id=job_id).update({
            'status': 'FAILED',
            'error_summary': str(e),
            'finished_at': datetime.utcnow()
        })
        return False