import os
from src.utils.logger import logger
from src.utils.config_loader import Config
from src.ingestion.ingest import ingest_file
from src.db.db_ops import init_db, ProcessingJob
from sqlalchemy.orm import Session
from datetime import datetime

def redact_sensitive(data):
    """Redact sensitive fields from log data."""
    if isinstance(data, dict):
        return {k: "REDACTED" if k in ['password', 'user', 'connection_string'] else redact_sensitive(v) for k, v in data.items()}
    return data

def main():
    # Load configuration
    try:
        Config.load('config/settings.yaml')
        db_type = Config.get('db.type')
        db_host = Config.get('db.host')
        db_port = Config.get('db.port')
        db_user = Config.get('db.user')
        db_password = Config.get('db.password')
        db_database = Config.get('db.database')
        providers = Config.get('providers', {})
        inbox_dir = Config.get('paths.inbox', 'data/inbox')

        required_fields = {'db.type': db_type, 'db.host': db_host, 'db.user': db_user, 'db.password': db_password, 'db.database': db_database}
        missing_fields = [key for key, value in required_fields.items() if value is None]
        if missing_fields:
            raise ValueError(f"Missing required config fields: {', '.join(missing_fields)}")
        
        if db_type.lower() != 'mysql':
            raise ValueError(f"Unsupported database type: {db_type}. Expected 'mysql'.")
        connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}"
    except (FileNotFoundError, RuntimeError, ValueError) as e:
        logger.error("Configuration loading failed", extra=redact_sensitive({"error": str(e)}))
        return
    
    # Initialize database
    try:
        SessionLocal = init_db(connection_string)
    except Exception as e:
        logger.error("Database initialization failed", extra=redact_sensitive({"error": str(e)}))
        return
    
    # Process files in inbox
    for filename in os.listdir(inbox_dir):
        file_path = os.path.join(inbox_dir, filename)
        if not os.path.isfile(file_path):
            continue
        
        # Create a new processing job
        with SessionLocal() as session:
            provider_key = filename.split('_')[0]
            report_period = filename.split('_')[-1].split('.')[0]  # e.g., 2023-09
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
            
            # Ingest file
            provider_mapping = providers.get(provider_key, {})
            success = ingest_file(file_path, provider_mapping, session, job_id)
            
            if success:
                logger.info("File processed successfully", extra={"file": filename, "job_id": job_id})
            else:
                logger.error("File processing failed", extra={"file": filename, "job_id": job_id})
                session.query(ProcessingJob).filter_by(job_id=job_id).update({
                    'status': 'FAILED',
                    'finished_at': datetime.utcnow()
                })
                session.commit()

if __name__ == "__main__":
    main()