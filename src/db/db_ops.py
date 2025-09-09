import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from src.db.db_engine import Database
from src.utils.logger import get_logger

logger = get_logger(__name__)

def insert_dataframe(df, provider, report_period: str, job_id: int):
    """
    Insert validated DataFrame into reports table.
    Links rows to processing job via job_id.
    Returns number of successfully inserted rows.
    """
    engine = Database.get_engine()
    rows_inserted = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            try:
                conn.execute(
                    sa.text("""
                        INSERT INTO reports (
                            id, name, fee, provider, reportPeriod,
                            ingested_at, status, job_id
                        )
                        VALUES (:id, :name, :fee, :provider, :reportPeriod,
                                NOW(), 'Processed', :job_id)
                    """),
                    {
                        "id": row["customer_id"],
                        "name": row["customer_name"],
                        "fee": row["fees"],
                        "provider": provider,
                        "reportPeriod": report_period,
                        "job_id": job_id
                    }
                )
                rows_inserted += 1
            except IntegrityError:
                logger.warning(f"Duplicate found for ID={row['customer_id']} in {report_period}")
            except Exception as e:
                logger.error(f"Insert failed for ID={row['customer_id']}: {e}")
    return rows_inserted


def create_processing_job(file_name, provider, report_period):
    """
    Create a new processing job entry.
    Returns the generated job_id.
    """
    engine = Database.get_engine()
    with engine.begin() as conn:
        result = conn.execute(
            sa.text("""
                INSERT INTO processing_jobs (file_name, provider, report_period, status)
                VALUES (:file_name, :provider, :report_period, 'STARTED')
            """),
            {
                "file_name": file_name,
                "provider": provider,
                "report_period": report_period
            }
        )
        job_id = result.lastrowid
        logger.info(f"Created processing job {job_id} for {file_name}")
        return job_id


def update_processing_job(job_id, status, rows_processed=0, rows_inserted=0,
                          rows_failed=0, error_summary=None):
    """
    Update a processing job with final results.
    """
    engine = Database.get_engine()
    with engine.begin() as conn:
        conn.execute(
            sa.text("""
                UPDATE processing_jobs
                SET status=:status,
                    rows_processed=:rows_processed,
                    rows_inserted=:rows_inserted,
                    rows_failed=:rows_failed,
                    error_summary=:error_summary,
                    finished_at=NOW()
                WHERE job_id=:job_id
            """),
            {
                "status": status,
                "rows_processed": rows_processed,
                "rows_inserted": rows_inserted,
                "rows_failed": rows_failed,
                "error_summary": error_summary,
                "job_id": job_id
            }
        )
        logger.info(f"Updated processing job {job_id} with status {status}")
