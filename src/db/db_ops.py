from sqlalchemy import create_engine, Column, Integer, String, Text, Enum, ForeignKey, TIMESTAMP, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import json
from src.utils.logger import logger
from datetime import datetime

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

def init_db(connection_string):
    try:
        engine = create_engine(connection_string)
        # Create tables in correct order
        ProcessingJob.__table__.create(engine, checkfirst=True)
        FailedRow.__table__.create(engine, checkfirst=True)
        Reports.__table__.create(engine, checkfirst=True)
        return sessionmaker(bind=engine)
    except Exception as e:
        logger.error("Database table creation failed", extra={"error": str(e)})
        raise

def log_failed_row(session, job_id, row_number, errors):
    failed_row = FailedRow(
        job_id=job_id,
        row_number=row_number,
        error_details=json.dumps(errors)
    )
    session.add(failed_row)
    session.commit()

def insert_dataframe(session, df, table_name, job_id):
    try:
        df.to_sql(table_name, session.bind, if_exists='append', index=False)
        session.query(ProcessingJob).filter_by(job_id=job_id).update({
            'status': 'SUCCESS',
            'rows_inserted': len(df),
            'rows_failed': 0,
            'finished_at': datetime.utcnow()
        })
        return True, None
    except Exception as e:
        session.query(ProcessingJob).filter_by(job_id=job_id).update({
            'status': 'FAILED',
            'error_summary': str(e),
            'finished_at': datetime.utcnow()
        })
        return False, str(e)