import os
from src.utils.logger import logger
from src.utils.config_loader import Config
from src.ingestion.ingest import ingest_file
from src.db.db_ops import init_db
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ProcessingJob(Base):
    __tablename__ = 'processing_jobs'
    id = Column(Integer, primary_key=True)
    file_name = Column(String)
    status = Column(String)
    rows_processed = Column(Integer)
    rows_inserted = Column(Integer)
    rows_failed = Column(Integer)
    error_summary = Column(String)

def main():
    # Load configuration
    try:
        Config.load('config/settings.yaml')  # Explicitly specify settings.yaml
        connection_string = Config.get('db.connection_string')
        if connection_string is None:
            raise ValueError("Missing 'db.connection_string' in config/settings.yaml")
        providers = Config.get('providers', {})
        inbox_dir = Config.get('paths.inbox', 'data/inbox')
    except (FileNotFoundError, RuntimeError, ValueError) as e:
        logger.error("Configuration loading failed", extra={"error": str(e)})
        return
    
    # Initialize database
    try:
        SessionLocal = init_db(connection_string)
    except Exception as e:
        logger.error("Database initialization failed", extra={"connection_string": connection_string, "error": str(e)})
        return
    
    # Process files in inbox
    for filename in os.listdir(inbox_dir):
        file_path = os.path.join(inbox_dir, filename)
        if not os.path.isfile(file_path):
            continue
        
        # Create a new processing job
        with SessionLocal() as session:
            job = ProcessingJob(
                file_name=filename,
                status='PROCESSING',
                rows_processed=0,
                rows_inserted=0,
                rows_failed=0,
                error_summary=''
            )
            session.add(job)
            session.commit()
            job_id = job.id
            
            # Ingest file
            provider_key = filename.split('_')[0]  # e.g., provider1_data.csv -> provider1
            provider_mapping = providers.get(provider_key, {})
            success = ingest_file(file_path, provider_mapping, session, job_id)
            
            if success:
                logger.info("File processed successfully", extra={"file": filename, "job_id": job_id})
            else:
                logger.error("File processing failed", extra={"file": filename, "job_id": job_id})
                session.query(ProcessingJob).filter_by(id=job_id).update({'status': 'FAILED'})

if __name__ == "__main__":
    main()