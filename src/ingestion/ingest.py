import pandas as pd
import os
from src.utils.logger import logger
from src.ingestion.file_ops import ensure_filename_suffix, move_file
from src.transformation.transform import normalize_and_validate
from src.db.db_ops import insert_dataframe, ProcessingJob
from datetime import datetime
from sqlalchemy.exc import OperationalError
from src.transformation.data_prep import prepare_data
from charset_normalizer import detect
from src.ingestion.file_ops import handle_fee_outliers


def ingest_file(file_path, provider_mapping, session, job_id):
    try:
        # Ensure filename has date suffix
        file_path = ensure_filename_suffix(file_path)

        # Read file with encoding
        try:
            if file_path.endswith('.csv'):
                with open(file_path, 'rb') as f:
                    result = detect(f.read(1024))
                encoding = result.get('encoding', 'utf-8')
                logger.debug(f"Detected encoding for {file_path}: {encoding}", extra={"job_id": job_id})
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='cp1255')
            elif file_path.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file_path)
                text = ' '.join(df.iloc[:, :].astype(str).values.flatten()).encode()
                result = detect(text)
                encoding = result.get('encoding', 'utf-8')
                logger.debug(f"Detected encoding for {file_path}: {encoding}", extra={"job_id": job_id})
            elif file_path.endswith('.txt'):
                df = pd.read_table(file_path)
            else:
                logger.error("Unsupported file format", extra={"job_id": job_id, "file": file_path})
                session.query(ProcessingJob).filter_by(job_id=job_id).update({
                    'status': 'FAILED',
                    'error_summary': 'Unsupported file format',
                    'finished_at': datetime.utcnow()
                })
                session.commit()
                move_file(file_path, 'data/failed', suffix='failed')
                return False
        except Exception as e:
            logger.error(f"Failed to read file", extra={"job_id": job_id, "file": file_path, "error": str(e)})
            session.query(ProcessingJob).filter_by(job_id=job_id).update({
                'status': 'FAILED',
                'error_summary': f'Failed to read file: {str(e)}',
                'finished_at': datetime.utcnow()
            })
            session.commit()
            move_file(file_path, 'data/failed', suffix='failed')
            return False

        provider_mapping['file_path'] = file_path
        result = prepare_data(df, provider_mapping, session, job_id)
        if result['status'] == 'failed':
            logger.error(f"Data preparation failed for {file_path}", extra={"job_id": job_id, "reason": result.get('reason', 'Unknown error')})
            session.query(ProcessingJob).filter_by(job_id=job_id).update({
                'status': 'FAILED',
                'error_summary': result.get('reason', 'Unknown error'),
                'finished_at': datetime.utcnow()
            })
            session.commit()
            move_file(file_path, 'data/failed', suffix='failed')
            return False

        # Handle outliers
        outlier_result = handle_fee_outliers(result)
        if outlier_result['status'] == 'failed':
            logger.error(f"Outlier handling failed for {file_path}: Too many outliers", extra={"job_id": job_id})
            session.query(ProcessingJob).filter_by(job_id=job_id).update({
                'status': 'FAILED',
                'error_summary': 'Too many outliers',
                'finished_at': datetime.utcnow()
            })
            session.commit()
            move_file(file_path, 'data/failed', suffix='failed')
            return False
        df = outlier_result['df']

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
            logger.debug(f"Updated ProcessingJob for job_id={job_id}, rows_processed={rows_processed}", extra={"job_id": job_id})
            session.commit()
        except OperationalError as e:
            logger.error(f"Database update failed for job_id={job_id}", extra={"job_id": job_id, "error": str(e)})
            session.rollback()
            move_file(file_path, 'data/failed', suffix='failed')
            return False

        if valid_df is None:
            logger.error(f"No valid rows after validation", extra={"job_id": job_id, "file": file_path, "errors": errors})
            session.query(ProcessingJob).filter_by(job_id=job_id).update({
                'status': 'FAILED',
                'error_summary': 'No valid rows after validation',
                'finished_at': datetime.utcnow()
            })
            session.commit()
            move_file(file_path, 'data/failed', suffix='failed')
            return False

        # Insert valid data
        success, error = insert_dataframe(session, valid_df, 'reports', job_id)
        if not success:
            logger.error(f"Failed to insert data to reports", extra={"job_id": job_id, "file": file_path, "error": error})
            session.query(ProcessingJob).filter_by(job_id=job_id).update({
                'status': 'FAILED',
                'error_summary': f'Failed to insert data: {error}',
                'finished_at': datetime.utcnow()
            })
            session.commit()
            move_file(file_path, 'data/failed', suffix='failed')
            return False

        # Move file to processed
        provider = provider_mapping.get('provider', 'Unknown')
        filename = os.path.basename(file_path)
        base, ext = os.path.splitext(filename)
        suffix = datetime.now().strftime('_%m%Y')
        status_suffix = '_processed' if rows_inserted > 0 else '_failed'
        new_filename = f"{base}_{provider}{suffix}{status_suffix}{ext}"
        target_dir = 'data/processed' if rows_inserted > 0 else 'data/failed'
        move_file(file_path, target_dir, suffix=status_suffix)
        logger.info(f"Moved {file_path} to {os.path.join(target_dir, new_filename)}", extra={"job_id": job_id})
        return True

    except Exception as e:
        logger.error(f"Ingestion failed", extra={"job_id": job_id, "file": file_path, "error": str(e)})
        move_file(file_path, 'data/failed', suffix='failed')
        session.query(ProcessingJob).filter_by(job_id=job_id).update({
            'status': 'FAILED',
            'error_summary': str(e),
            'finished_at': datetime.utcnow()
        })
        session.commit()
        return False