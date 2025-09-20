from sqlalchemy import create_engine as sa_create_engine, Column, Integer, String, DateTime, Text, Enum,ForeignKey,TIMESTAMP,UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
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
    id = Column(String(50), nullable=False)
    first_name = Column(String(255))
    last_name = Column(String(255))
    fee = Column(Integer)
    provider = Column(String(50), nullable=False)
    reportPeriod = Column(String(7), nullable=False)
    ingested_at = Column(TIMESTAMP, default=datetime.utcnow)
    status = Column(String(20))
    job_id = Column(Integer, ForeignKey('processing_jobs.job_id'))
    __table_args__ = (UniqueConstraint('id', 'provider', 'reportPeriod', name='uix_customer_provider_period'),)
    
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

def insert_dataframe(session, df, table_name, job_id):
    """Insert DataFrame rows into the specified table."""
    try:
        df.to_sql(table_name, con=session.bind, if_exists='append', index=False)
        logger.info(f"Inserted {len(df)} rows into {table_name} for job_id={job_id}")
        return True, None
    except Exception as e:
        logger.error(f"Failed to insert data to {table_name} for job_id={job_id}: {str(e)}")
        return False, str(e)

