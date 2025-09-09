import streamlit as st
import pandas as pd
import os
from src.utils.logger import logger
from src.utils.config_loader import Config
from src.ingestion.ingest import ingest_file
from src.ingestion.file_ops import move_file
from src.db.db_ops import init_db, ProcessingJob, FailedRow, Reports
from sqlalchemy.orm import Session
from sqlalchemy import select
from datetime import datetime

def redact_sensitive(data):
    """Redact sensitive fields from log data."""
    if isinstance(data, dict):
        return {k: "REDACTED" if k in ['password', 'user', 'connection_string'] else redact_sensitive(v) for k, v in data.items()}
    return data

def main():
    st.title("ETL Pipeline Dashboard")

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
        failed_dir = Config.get('paths.failed', 'data/failed')

        required_fields = {'db.type': db_type, 'db.host': db_host, 'db.user': db_user, 'db.password': db_password, 'db.database': db_database}
        missing_fields = [key for key, value in required_fields.items() if value is None]
        if missing_fields:
            raise ValueError(f"Missing required config fields: {', '.join(missing_fields)}")
        
        if db_type.lower() != 'mysql':
            raise ValueError(f"Unsupported database type: {db_type}. Expected 'mysql'.")
        connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}"
    except (FileNotFoundError, RuntimeError, ValueError) as e:
        st.error(f"Configuration error: {str(e)}")
        logger.error("Configuration loading failed", extra=redact_sensitive({"error": str(e)}))
        return

    # Initialize database
    try:
        SessionLocal = init_db(connection_string)
    except Exception as e:
        st.error(f"Database initialization failed: {str(e)}")
        logger.error("Database initialization failed", extra=redact_sensitive({"error": str(e)}))
        return

    # Display Processing Jobs
    st.header("Processing Jobs")
    with SessionLocal() as session:
        jobs_query = select(ProcessingJob)
        jobs_df = pd.read_sql(jobs_query, session.bind)
        st.dataframe(jobs_df, use_container_width=True)

    # Display Failed Rows
    st.header("Failed Rows")
    with SessionLocal() as session:
        failed_query = select(FailedRow)
        failed_df = pd.read_sql(failed_query, session.bind)
        st.dataframe(failed_df, use_container_width=True)

    # Display Reports
    st.header("Reports")
    with SessionLocal() as session:
        reports_query = select(Reports)
        reports_df = pd.read_sql(reports_query, session.bind)
        st.dataframe(reports_df, use_container_width=True)

    # Reprocess Failed Files
    st.header("Reprocess Failed Files")
    failed_files = [f for f in os.listdir(failed_dir) if os.path.isfile(os.path.join(failed_dir, f))]
    if failed_files:
        selected_file = st.selectbox("Select a failed file to reprocess", failed_files)
        if st.button("Reprocess"):
            with SessionLocal() as session:
                # Move file to inbox
                failed_path = os.path.join(failed_dir, selected_file)
                inbox_path = os.path.join(inbox_dir, selected_file.replace('_failed', ''))
                try:
                    move_file(failed_path, inbox_dir)
                    logger.info("Moved file for reprocessing", extra={"file": selected_file, "from": failed_path, "to": inbox_path})
                    
                    # Create new processing job
                    provider_key = selected_file.split('_')[0]
                    report_period = selected_file.split('_')[-1].split('.')[0]
                    job = ProcessingJob(
                        file_name=selected_file,
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
                    success = ingest_file(inbox_path, provider_mapping, session, job_id)
                    
                    if success:
                        st.success(f"Successfully reprocessed {selected_file}")
                        logger.info("File reprocessed successfully", extra={"file": selected_file, "job_id": job_id})
                    else:
                        st.error(f"Failed to reprocess {selected_file}")
                        logger.error("File reprocessing failed", extra={"file": selected_file, "job_id": job_id})
                        session.query(ProcessingJob).filter_by(job_id=job_id).update({
                            'status': 'FAILED',
                            'finished_at': datetime.utcnow()
                        })
                        session.commit()
                except Exception as e:
                    st.error(f"Error reprocessing {selected_file}: {str(e)}")
                    logger.error("Reprocessing error", extra={"file": selected_file, "error": str(e)})
    else:
        st.write("No failed files available for reprocessing.")

if __name__ == "__main__":
    main()