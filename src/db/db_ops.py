from sqlalchemy import create_engine as sa_create_engine, Column, Integer, String, DateTime, Text, Enum,ForeignKey,TIMESTAMP,UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import text
import pandas as pd
import json
from src.utils.logger import logger
from datetime import datetime
from src.utils.config_loader import Config

Base = declarative_base()

class ProcessingJob(Base):
    __tablename__ = 'processing_jobs'
    job_id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String(255), nullable=False)
    provider = Column(String(255))
    report_period = Column(String(7))
    rows_processed = Column(Integer, default=0)
    rows_inserted = Column(Integer, default=0)
    rows_failed = Column(Integer, default=0)
    status = Column(Enum('STARTED', 'SUCCESS', 'FAILED', 'PARTIAL', name='job_status'), nullable=False, default='STARTED')
    error_summary = Column(Text)
    started_at = Column(TIMESTAMP, default=datetime.utcnow)
    finished_at = Column(TIMESTAMP)

class FailedRow(Base):
    __tablename__ = 'failed_rows'
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('processing_jobs.job_id'))
    row_number = Column(Integer)
    error_details = Column(Text)

class Reports(Base):
    __tablename__ = 'reports'
    row_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(50), nullable=False)
    first_name = Column(String(255))
    last_name = Column(String(255))
    monthly_fee = Column(Integer)
    provider_name = Column(String(50), nullable=False)
    paid_month = Column(String(7), nullable=False)
    ingested_at = Column(TIMESTAMP, default=datetime.utcnow)
    status = Column(String(20))
    job_id = Column(Integer, ForeignKey('processing_jobs.job_id'))
    # In Reports class, line ~32
    __table_args__ = (UniqueConstraint('customer_id', 'provider_name', 'paid_month', name='uix_customer_provider_period'),)
    # __table_args__ = (UniqueConstraint('id', 'provider', 'reportPeriod', name='uix_customer_provider_period'),)
    
class Clients(Base):
    __tablename__ = 'clients'
    client_id = Column(String(9), primary_key=True)
    first_name = Column(String(255), nullable=False)
    last_name = Column(String(255), nullable=False)
    provider = Column(String(255), nullable=False)

def init_db(engine):
    """Initialize the database by creating all defined tables."""
    try:
        Base.metadata.create_all(engine)
        logger.info("Database tables initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

def create_engine():
    """Create a SQLAlchemy engine from configuration."""
    db_config = Config.get('database', {})
    connection_string = (
        f"mysql+mysqlconnector://{db_config.get('user')}:{db_config.get('password')}@"
        f"{db_config.get('host')}:{db_config.get('port')}/{db_config.get('database')}"
    )
    try:
        return sa_create_engine(connection_string)
    except Exception as e:
        logger.error(f"Failed to create engine: {str(e)}")
        raise

def create_session(engine):
    """Create a SQLAlchemy session bound to the provided engine."""
    try:
        Session = sessionmaker(bind=engine)
        return Session()
    except Exception as e:
        logger.error(f"Failed to create session: {str(e)}")
        raise

def log_failed_row(session, job_id, row, reason):
    """Log a failed row to the database or a log file."""
    try:
        # Assuming a failed_rows table or log file; adjust as per your implementation
        logger.error(f"Failed row for job_id={job_id}: {row.to_dict()}, reason: {reason}")
        # Example: Insert into a failed_rows table (uncomment if applicable)
        # session.execute("INSERT INTO failed_rows (job_id, row_data, reason) VALUES (:job_id, :row_data, :reason)",
        #                 {'job_id': job_id, 'row_data': str(row.to_dict()), 'reason': reason})
        session.commit()
    except Exception as e:
        logger.error(f"Failed to log failed row for job_id={job_id}: {str(e)}")
        session.rollback()
        raise
    
# In db_ops.py, replace insert_dataframe (around line ~107)
def insert_dataframe(session, df, table_name, job_id):
    try:
        row_list = df.to_dict('records')  # List of dicts
        duplicates = []
        for row_dict in row_list:
            # Remove job_id and status, not in reports table
            insert_dict = {k: v for k, v in row_dict.items() if k not in ['job_id', 'status']}
            # Check for duplicate with case-insensitive provider_name
            query = text("""
                SELECT COUNT(*) FROM reports
                WHERE customer_id = :customer_id
                AND LOWER(provider_name) = LOWER(:provider_name)
                AND paid_month = :paid_month
            """)
            result = session.execute(query, {
                'customer_id': insert_dict['customer_id'],
                'provider_name': insert_dict['provider_name'],
                'paid_month': insert_dict['paid_month']
            }).scalar()
            if result > 0:
                duplicates.append({'row_index': row_list.index(row_dict), 'data': row_dict, 'errors': ['Duplicate entry']})
                continue
            session.add(Reports(**insert_dict))
        session.commit()
        logger.debug(f"Inserted {len(row_list) - len(duplicates)} rows for job_id={job_id}, duplicates={len(duplicates)}")
        return True, None, duplicates
    except Exception as e:
        logger.error(f"Failed to insert row for job_id={job_id}: {str(e)}")
        session.rollback()
        return False, str(e), []