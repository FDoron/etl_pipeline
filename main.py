import os
import re
from datetime import datetime
from glob import glob
from src.utils.config_loader import Config
from src.utils.logger import logger
from src.db.db_ops import ProcessingJob, create_engine, create_session
from src.ingestion.file_ops import rename_file_with_id_column, ensure_filename_suffix
from src.ingestion.ingest import ingest_file


from src.utils import file_ops



def extract_provider_and_period(filename):
    """Extract provider and report period from filename."""
    parts = re.split(r'[\s_]+', filename)
    parts = [p for p in parts if p not in ['failed', 'processed', 'archived'] and not re.match(r'^\d{2}\d{4}$', p)]
    provider = '_'.join(parts[:-1]) if parts else filename.split('.')[0]
    period = next((p for p in parts if re.match(r'^\d{2}\d{4}$', p)), datetime.now().strftime('%m%Y'))
    return provider, period

def process_file(file_path, session):
    """Process a single file through the pipeline."""
    filename = os.path.basename(file_path)
    provider_key, report_period = extract_provider_and_period(filename)
    logger.info(f"Processing file: {filename}, provider: {provider_key}, period: {report_period}")
    
    # Create job
    job = ProcessingJob(
        file_name=filename,
        provider=provider_key,
        report_period=report_period,
        status='STARTED',
        rows_processed=0,
        rows_inserted=0,
        rows_failed=0,
        started_at=datetime.utcnow()
    )
    session.add(job)
    session.commit()
    job_id = job.job_id
    
    # Rename file
    rename_result = rename_file_with_id_column(file_path, session, job_id)
    if not rename_result:
        logger.error(f"Renaming failed for {file_path}", extra={"job_id": job_id})
        session.query(ProcessingJob).filter_by(job_id=job_id).update({
            'status': 'FAILED',
            'error_summary': 'Failed to rename file',
            'finished_at': datetime.utcnow()
        })
        session.commit()
        return False
    
    file_path, majority_provider = rename_result
    file_path = ensure_filename_suffix(file_path)
    
    # Ingest file
    success = ingest_file(file_path, {'column_mapping': Config.get('column_mapping', {}), 'provider': majority_provider, 'report_period': report_period}, session, job_id)
    if not success:
        logger.error(f"Ingestion failed for {file_path}", extra={"job_id": job_id})
        file_ops.move_file(file_path, "data/failed", job_id)  # Move to failed if ingest_file fails
        session.query(ProcessingJob).filter_by(job_id=job_id).update({
            'status': 'FAILED',
            'error_summary': 'Ingestion failed',
            'finished_at': datetime.utcnow()
        })
        session.commit()
        return False
    if file_ops.move_file(file_path, "data/processed", job_id):  # Move only if ingest_file succeeds
        session.query(ProcessingJob).filter_by(job_id=job_id).update({
            'status': 'SUCCESS',  # Use 'SUCCESS' to avoid truncation
            'finished_at': datetime.utcnow()
        })
        session.commit()
        logger.info(f"Successfully processed {file_path}", extra={"job_id": job_id})
        return True
    else:
        logger.error(f"Failed to move {file_path} to data/processed", extra={"job_id": job_id})
        session.query(ProcessingJob).filter_by(job_id=job_id).update({
            'status': 'FAILED',
            'error_summary': 'File move failed',
            'finished_at': datetime.utcnow()
        })
        session.commit()
        return False
         
    logger.info(f"Successfully processed {file_path}", extra={"job_id": job_id})
    return True

def main():
    Config.load('config/settings.yaml')
    logger.debug("Loaded settings.yaml, inbox path: %s", Config.get('paths', {}).get('inbox', 'data/inbox'))
    engine = create_engine()
    session = create_session(engine)
    
    inbox_dir = Config.get('paths', {}).get('inbox', 'data/inbox')
    for file_path in glob(os.path.join(inbox_dir, '*')):
        if not os.path.isfile(file_path):
            continue
        try:
            process_file(file_path, session)
        except Exception as e:
            logger.error(f"Failed to process {file_path}", extra={"error": str(e)})
            session.rollback()
            continue
    session.close()
    
    
if __name__ == '__main__': 
    main()