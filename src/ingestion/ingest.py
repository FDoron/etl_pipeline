import os
from src.utils.logger import get_logger
from src.utils.config_loader import Config
from src.transformation.transform import normalize_and_validate
from src.ingestion.file_ops import ingest_file, ensure_month_suffix, move_file, archive_file

logger = get_logger(__name__)
config = Config.load("config/settings.yaml")


def ingest_files(folder_path: str):
    """
    Complete ingestion workflow for all files in a folder.
    Returns list of dicts with file status, rows, errors.
    """
    processed_folder = config["paths"]["processed"]
    failed_folder = config["paths"]["failed"]
    archive_folder = config["paths"]["archive"]

    results = []

    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

    for file in files:
        try:
            # -------------------------------
            # Ensure suffix _YYYY-MM (rename if needed)
            # -------------------------------
            file_with_suffix = ensure_month_suffix(file, folder_path)
            full_path = os.path.join(folder_path, file_with_suffix)

            # -------------------------------
            # Archive original
            # -------------------------------
            archive_file(full_path, archive_folder)

            # -------------------------------
            # Ingest raw file
            # -------------------------------
            df = ingest_file(full_path)
            if df is None:
                move_file(full_path, failed_folder, "failed")
                results.append({
                    "file": file_with_suffix,
                    "status": "FAILED",
                    "rows": 0,
                    "errors": ["Failed to read file"]
                })
                continue

            # -------------------------------
            # Transform & validate
            # -------------------------------
            df, valid, errors = normalize_and_validate(
                df,
                required_columns=config["required_columns"],
                column_mapping=config["column_mapping"]
            )

            if valid:
                move_file(full_path, processed_folder, "processed")
                results.append({
                    "file": file_with_suffix,
                    "status": "SUCCESS",
                    "rows": len(df),
                    "errors": []
                })
            else:
                move_file(full_path, failed_folder, "failed")
                results.append({
                    "file": file_with_suffix,
                    "status": "FAILED",
                    "rows": len(df),
                    "errors": errors
                })

        except Exception as e:
            logger.error(f"Unexpected error processing {file}: {e}")
            results.append({
                "file": file,
                "status": "FAILED",
                "rows": 0,
                "errors": [str(e)]
            })

    return results
