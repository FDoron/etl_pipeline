import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
    
# src/main.py
from pathlib import Path
from src.utils.logger import get_logger
from src.utils.config_loader import Config
from src.ingestion.ingest import ingest_files, move_file, archive_file, ensure_month_suffix
from src.ingestion.file_ops import ensure_month_suffix
from src.transformation.transform import normalize_and_validate
from src.db.db_ops import insert_dataframe, create_processing_job, update_processing_job
from src.ingestion.ingest import ingest_files


logger = get_logger(__name__)
config = Config.load("config/settings.yaml")

summary = ingest_files(config["paths"]["inbox"])

def main():
    logger.info("Pipeline started.")

    inbox_path = config["paths"]["inbox"]
    processed_folder = config["paths"]["processed"]
    failed_folder = config["paths"]["failed"]

    files = [f for f in os.listdir(inbox_path) if os.path.isfile(os.path.join(inbox_path, f))]

    for file in files:
        # Ensure file name has _YYYY-MM suffix
        # Step 1: Ensure file name has _YYYY-MM suffix (renames if needed)
        file_with_suffix = ensure_month_suffix(file, inbox_path)
        file_path = os.path.join(inbox_path, file_with_suffix)

        # Step 2: Archive the renamed/original file
        archive_file(file_path)


        # Create processing job
        provider = file.split("_")[0]
        report_period = file.split("_")[-1].replace(".csv","").replace(".xlsx","")
        job_id = create_processing_job(file, provider, report_period)

        # Ingest
        df = ingest_files(file_path)
        if df is None:
            move_file(file_path, failed_folder, "failed")
            update_processing_job(job_id, status="FAILED", rows_processed=0, rows_inserted=0,
                                  rows_failed=0, error_summary="Failed to read file")
            continue

        # Transform & validate
        df, valid, errors = normalize_and_validate(
            df,
            required_columns=config["required_columns"],
            column_mapping=config["column_mapping"]
        )

        rows_processed = len(df)
        rows_inserted = 0
        rows_failed = 0
        error_summary = None

        if valid:
            # Insert into DB
            rows_inserted = insert_dataframe(df, provider, report_period, job_id)
            move_file(file_path, processed_folder, "processed")
            status = "SUCCESS"
        else:
            move_file(file_path, failed_folder, "failed")
            rows_failed = len(df)
            error_summary = str(errors)
            status = "FAILED"

        # Update processing job
        update_processing_job(
            job_id,
            status=status,
            rows_processed=rows_processed,
            rows_inserted=rows_inserted,
            rows_failed=rows_failed,
            error_summary=error_summary
        )

    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    main()
