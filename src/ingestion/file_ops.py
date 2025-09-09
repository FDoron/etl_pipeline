import os
import shutil
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)

# --- Ensure file name ends with _YYYY-MM (File only) ---
def ensure_month_suffix(file_name: str, folder: str) -> str:  # file_ops.py
    base, ext = os.path.splitext(file_name)
    parts = base.split("_")
    try:
        # Check if last part is YYYY-MM
        datetime.strptime(parts[-1], "%Y-%m")
        return file_name  # already correct
    except ValueError:
        # Add current YYYY-MM
        suffix = datetime.now().strftime("%Y-%m")
        new_name = f"{base}_{suffix}{ext}"
        old_path = os.path.join(folder, file_name)
        new_path = os.path.join(folder, new_name)
        os.rename(old_path, new_path)
        logger.info(f"Renamed file {file_name} -> {new_name}")
        return new_name

# --- Move a single file to processed/failed folder ---
def move_file(src_path: str, dest_folder: str, status: str) -> str:  # file_ops.py
    os.makedirs(dest_folder, exist_ok=True)
    base = os.path.basename(src_path)
    for suffix in ["_processed", "_failed"]:
        if suffix in base:
            base = base.replace(suffix, "")
    name, ext = os.path.splitext(base)
    new_name = f"{name}_{status}{ext}"
    dest_path = os.path.join(dest_folder, new_name)
    shutil.move(src_path, dest_path)
    logger.info(f"Moved {src_path} -> {dest_path}")
    return dest_path

# --- Archive a single file ---
def archive_file(src_path: str, archive_folder: str) -> str:  # file_ops.py
    os.makedirs(archive_folder, exist_ok=True)
    dest_path = os.path.join(archive_folder, os.path.basename(src_path))
    shutil.copy2(src_path, dest_path)
    logger.info(f"Archived file: {dest_path}")
    return dest_path

# --- Ingest raw file ---
import pandas as pd
def ingest_file(file_path: str) -> pd.DataFrame | None:  # file_ops.py
    try:
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_path.endswith((".xls", ".xlsx")):
            df = pd.read_excel(file_path)
        elif file_path.endswith(".txt"):
            df = pd.read_csv(file_path, delimiter="\t")
        else:
            logger.error(f"Unsupported file type: {file_path}")
            return None
        df["ingested_at"] = pd.Timestamp.now()
        logger.info(f"Ingested file {file_path} with {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to ingest {file_path}: {e}")
        return None
