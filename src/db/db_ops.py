from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json

Base = declarative_base()

class ProcessingJob(Base):
    __tablename__ = 'processing_jobs'
    id = Column(Integer, primary_key=True)
    file_name = Column(String)
    status = Column(String)
    rows_processed = Column(Integer)
    rows_inserted = Column(Integer)
    rows_failed = Column(Integer)
    error_summary = Column(Text)

class FailedRow(Base):
    __tablename__ = 'failed_rows'
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('processing_jobs.id'))
    row_number = Column(Integer)
    error_details = Column(Text)

def init_db(connection_string):
    engine = create_engine(connection_string)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)

def log_failed_row(session, job_id, row_number, errors):
    failed_row = FailedRow(
        job_id=job_id,
        row_number=row_number,
        error_details=json.dumps(errors)
    )
    session.add(failed_row)

def insert_dataframe(session, df, table_name, job_id):
    """Insert dataframe to DB with transaction management."""
    try:
        with session.begin():
            df.to_sql(table_name, session.bind, if_exists='append', index=False)
            session.query(ProcessingJob).filter_by(id=job_id).update({
                'status': 'SUCCESS',
                'rows_inserted': len(df),
                'rows_failed': 0
            })
        return True, None
    except Exception as e:
        session.rollback()
        session.query(ProcessingJob).filter_by(id=job_id).update({
            'status': 'FAILED',
            'error_summary': str(e)
        })
        return False, str(e)