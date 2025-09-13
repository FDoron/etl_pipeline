import pandas as pd
from src.utils.logger import logger
from src.ingestion.file_ops import ensure_filename_suffix, move_file
from src.transformation.transform import normalize_and_validate
from src.db.db_ops import insert_dataframe, ProcessingJob
from datetime import datetime
from sqlalchemy.exc import OperationalError

def ingest_file(file_path, provider_mapping, session, job_id):
    try:
        # Ensure filename has date suffix
        file_path = ensure_filename_suffix(file_path)
        
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file_path)
            elif file_path.endswith('.txt'):
                df = pd.read_table(file_path)
        # else:
        #     raise ValueError("Unsupported file format")
        except Exception as e:
            logger.error("Failed to read CSV", extra={"file": file_path, "error": str(e)}) 
            move_file(file_path, 'data/failed', suffix='failed')
            return False
   
        valid_df, errors = normalize_and_validate(df, {'column_mapping': provider_mapping['column_mapping'], 'provider': provider_mapping['provider']}, session, job_id)
        
        # Update job status
        rows_processed = len(df)
        rows_failed = len(errors)
        rows_inserted = len(valid_df) if valid_df is not None else 0
        
        try:
            session.query(ProcessingJob).filter_by(job_id=job_id).update({
                'rows_processed': rows_processed,
                'rows_failed': rows_failed,
                'rows_inserted': rows_inserted,
                'status': 'PARTIAL' if rows_failed > 0 and rows_inserted > 0 else ('SUCCESS' if rows_inserted > 0 else 'FAILED'),
                'finished_at': datetime.utcnow()
            }, synchronize_session='evaluate')
            logger.debug(f"Updated ProcessingJob for job_id={job_id}, rows_processed={rows_processed}")
            session.commit()
        except OperationalError as e:
            logger.error(f"Database update failed for job_id={job_id}", extra={"error": str(e)})
            session.rollback()
            raise
        
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