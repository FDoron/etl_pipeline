import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from src.utils.config_loader import Config
from src.db.db_ops import ProcessingJob

def get_db_session(config):
    """Create SQLAlchemy session from config."""
    db_config = config.get('database', {})
    db_type = db_config.get('type')
    if not db_type:
        st.error("Database type not specified in settings.yaml. Expected 'mysql'.")
        st.stop()
    dialect_map = {'mysql': 'mysql+mysqlconnector'}
    driver = dialect_map.get(db_type)
    if not driver:
        st.error(f"Unsupported database type '{db_type}' in settings.yaml. Expected 'mysql'.")
        st.stop()
    port = db_config.get('port', 3306)
    connection_string = f"{driver}://{db_config.get('user')}:{db_config.get('password')}@{db_config.get('host')}:{port}/{db_config.get('database')}"
    try:
        from sqlalchemy import create_engine
        engine = create_engine(connection_string)
        return sessionmaker(bind=engine)()
    except Exception as e:
        st.error(f"Failed to create database session: {str(e)}")
        st.stop()

def fetch_job_data(session, status_filter=None, days_filter=None):
    """Fetch job data from ProcessingJob table with optional filters."""
    query = session.query(ProcessingJob)
    if status_filter and status_filter != "All":
        query = query.filter(ProcessingJob.status == status_filter)
    if days_filter:
        cutoff_date = datetime.utcnow() - timedelta(days=days_filter)
        query = query.filter(ProcessingJob.finished_at >= cutoff_date)
    jobs = query.all()
    job_data = [{
        'job_id': job.job_id,
        'file_name': job.file_name,
        'provider': job.provider,
        'report_period': job.report_period,
        'rows_processed': job.rows_processed,
        'rows_inserted': job.rows_inserted,
        'rows_failed': job.rows_failed,
        'status': job.status,
        'error_summary': str(job.error_summary or ''),
        'started_at': job.started_at,
        'finished_at': job.finished_at
    } for job in jobs]
    return pd.DataFrame(job_data)

def highlight_invalid_rows(row):
    """Style invalid rows red based on errors column."""
    return ['background-color: #FFCCCC' if pd.notna(row.get('errors', '')) else '' for _ in row]

def render_button(row):
    """Render Upload button for PARTIAL rows."""
    if row['status'] == 'PARTIAL' and Config.get('ingestion.skip_reports_insert', False):
        return f'<button>Upload</button>'
    return ''

st.title("ETL Pipeline Dashboard")
Config.load()
session = get_db_session(Config.load())

# Filters
st.subheader("Filter Jobs")
col1, col2 = st.columns(2)
status_options = ['All', 'STARTED', 'SUCCESS', 'FAILED', 'PARTIAL']
status_filter = col1.selectbox("Filter by Status", status_options, index=0)
days_filter = col2.number_input("Filter by Last X Days", min_value=1, max_value=365, value=7, step=1)

# Display ProcessingJob table
st.subheader("Processing Jobs")
job_data = fetch_job_data(session, status_filter, days_filter)
if job_data.empty:
    st.warning("No jobs match the selected filters.")
else:
    display_data = job_data.copy()
    if Config.get('ingestion.skip_reports_insert', False):
        display_data['Action'] = display_data.apply(render_button, axis=1)
    st.dataframe(display_data, use_container_width=True, hide_index=True)

# Highlighted DataFrame under table
st.subheader("View Highlighted Data")
job_ids = job_data['job_id'].tolist()
if job_ids:
    selected_job_id = st.selectbox("Select Job ID", job_ids)
    if selected_job_id:
        job = job_data[job_data['job_id'] == selected_job_id].iloc[0]
        file_name_base = os.path.basename(job['file_name']).replace('.csv', '').replace('.xlsx', '')
        file_path = f'data/review/{file_name_base}_highlighted.xlsx'
        if os.path.exists(file_path):
            df = pd.read_excel(file_path)
            st.dataframe(df.style.apply(highlight_invalid_rows, axis=1), use_container_width=True)
        else:
            st.error(f"File not found: {file_path}")
else:
    st.warning("No jobs available to view.")

session.close()