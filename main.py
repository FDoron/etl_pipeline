# main.py
import os
import sys

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger
from src.utils.config_loader import Config
from src.ingestion.ingest import ingest_files
from src.db.db_ops import create_processing_job, update_processing_job, insert_dataframe

logger = get_logger(__name__)
config = Config.load("config/settings.yaml")


def main():
    logger.info("Pipeline started.")

    inbox_path = config["paths"]["inbox"]
    processed_folder = config["paths"]["processed"]
    failed_folder = config["paths"]["failed"]

    # Loop over files in the folder
    files = [f for f in os.listdir(inbox_path) if os.path.isfile(os.path.join(inbox_path, f))]

    for file in files:
        file_path = os.path.join(inbox_path, file)

        # Extract provider and report_period for job
        provider = file.split("_")[0]
        # Extract last part and remove extension, e.g., '2025-09.xlsx' -> '2025-09'
        report_period = os.path.splitext(file.split("_")[-1])[0]

        # Create processing job entry
        job_id = create_processing_job(file, provider, report_period)

        # Ingest the file (returns df or None if fail)
        df = ingest_files(inbox_path)  # <-- pass folder, not full file

        if df is None:
            logger.error(f"Ingestion failed: {file}")
            update_processing_job(job_id,
                                  status="FAILED",
                                  rows_processed=0,
                                  rows_inserted=0,
                                  rows_failed=0,
                                  error_summary="File failed ingestion or validation")
            continue

        # Insert into DB
        rows_inserted = insert_dataframe(df, provider, report_period, job_id)
        rows_processed = len(df)
        rows_failed = 0
        status = "SUCCESS"

        # Update processing job
        update_processing_job(job_id,
                              status=status,
                              rows_processed=rows_processed,
                              rows_inserted=rows_inserted,
                              rows_failed=rows_failed,
                              error_summary=None)

        logger.info(f"Finished processing {file} - {status}")

    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    main()
