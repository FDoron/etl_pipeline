import os
from src.utils.logger import get_logger
from src.utils.config_loader import Config
from src.transformation.transform import normalize_and_validate
from src.ingestion.file_ops import ingest_file, ensure_month_suffix, move_file, archive_file

logger = get_logger(__name__)
config = Config.load("config/settings.yaml")


def ingest_files(inbox_path: str):
    processed_folder = config["paths"]["processed"]
    failed_folder = config["paths"]["failed"]
    archive_folder = config["paths"]["archive"]

    results = []
    files = [f for f in os.listdir(inbox_path) if os.path.isfile(os.path.join(inbox_path, f))]

    for file in files:
        # --- Ensure _YYYY-MM suffix ---
        file_with_suffix = ensure_month_suffix(file, inbox_path)
        full_path = os.path.join(inbox_path, file_with_suffix)

        # --- Archive original ---
        archive_file(full_path, archive_folder)

        # --- Ingest raw ---
        df = ingest_file(full_path)
        if df is None:
            move_file(full_path, failed_folder, "failed")
            results.append({"file": file_with_suffix, "status": "failed_ingest", "rows": 0})
            continue

        # --- Transform & validate ---
        df, valid, errors = normalize_and_validate(
            df,
            required_columns=config["required_columns"],
            column_mapping=config["column_mapping"]
        )

        if valid:
            move_file(full_path, processed_folder, "processed")
            results.append({"file": file_with_suffix, "status": "processed", "rows": len(df)})
        else:
            move_file(full_path, failed_folder, "failed")
            results.append({
                "file": file_with_suffix,
                "status": "failed_validation",
                "rows": len(df),
                "errors": errors
            })

    return results


if __name__ == "__main__":
    inbox_path = config["paths"]["inbox"]
    summary = ingest_files(inbox_path)
    for s in summary:
        print(s)
