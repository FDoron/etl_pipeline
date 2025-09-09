import os
import sys

# -------------------------------
# Ensure project root in sys.path
# -------------------------------
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger
from src.utils.config_loader import Config
from src.ingestion.ingest import ingest_files, move_file, archive_file  # updated import
from src.db.db_ops import insert_dataframe, create_processing_job, update_processing_job

logger = get_logger(__name__)
config = Config.load("config/settings.yaml")


def main():
    logger.info("Pipeline started.")

    inbox_path = config["paths"]["inbox"]
    processed_folder = config["paths"]["processed"]
    failed_folder = config["paths"]["failed"]

    # -------------------------------
    # Ingest all files in folder
    # -------------------------------
    ingestion_results = ingest_files(inbox_path)

    for result in ingestion_results:
        file_name = result["file"]
        status = result["status"]
        errors = result["errors"]
        rows = result["rows"]

        # Extract provider and report_period from file name
        provider = file_name.split("_")[0]
        report_period = file_name.split("_")[-1].replace(".csv", "").replace(".xlsx", "")

        # Create a processing job for this file
        job_id = create_processing_job(file_name, provider, report_period)

        if status == "SUCCESS":
            # File was successfully validated, insert into DB
            df = result.get("df")  # you may store df in result in ingest_files if needed
            # If not stored, re-ingest the file to get df
            df = ingest_file(os.path.join(inbox_path, file_name))  # safe re-read
            df, valid, errors_check = normalize_and_validate(
                df,
                required_columns=config["required_columns"],
                column_mapping=config["column_mapping"]
            )
            rows_inserted = insert_dataframe(df, provider, report_period, job_id)
            update_processing_job(
                job_id,
                status="SUCCESS",
                rows_processed=rows,
                rows_inserted=rows_inserted,
                rows_failed=0,
                error_summary=None
            )
        else:
            # Failed ingestion or validation
            update_processing_job(
                job_id,
                status="FAILED",
                rows_processed=rows,
                rows_inserted=0,
                rows_failed=rows,
                error_summary=str(errors)
            )

    logger.info("Pipeline finished.")


if __name__ == "__main__":
    main()
