import os
import pandas as pd
from src.utils.logger import log_action
from src.ingestion.file_ops import ensure_filename_suffix, move_file
from src.transformation.transform import normalize_and_validate
from src.db.db_ops import insert_dataframe, init_db
from sqlalchemy.exc import IntegrityError, DatabaseError
import pandas.errors

def ingest_file(file_path, provider_mapping, session, job_id):
    """Ingest a single file with specific exception handling."""
    try:
        file_path = ensure_filename_suffix(file_path)
        log_action("File ingestion started", {"file": file_path, "job_id": job_id})
        
        # Read file with specific error handling
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file_path)
            elif file_path.endswith('.txt'):
                df = pd.read_table(file_path)
            else:
                raise ValueError("Unsupported file format")
        except pandas.errors.ParserError:
            log_action("File parsing failed", {"file": file_path, "error": "Invalid file format or structure"})
            move_file(file_path, 'data/failed', 'failed')
            return False
        except FileNotFoundError:
            log_action("File not found", {"file": file_path})
            return False
        
        # Validate and transform
        valid_df, errors = normalize_and_validate(df, provider_mapping, session, job_id)
        if valid_df is None:
            log_action("Validation failed", {"file": file_path, "errors": errors})
            move_file(file_path, 'data/failed', 'failed')
            return False
        
        # Insert to DB
        success, error = insert_dataframe(session, valid_df, 'reports', job_id)
        if not success:
            log_action("Database insertion failed", {"file": file_path, "error": error})
            move_file(file_path, 'data/failed', 'failed')
            return False
        
        log_action("File ingestion completed", {"file": file_path, "rows_inserted": len(valid_df)})
        move_file(file_path, 'data/processed', 'processed')
        return True
    
    except PermissionError as e:
        log_action("Permission error", {"file": file_path, "error": str(e)})
        return False
    except OSError as e:
        log_action("OS error", {"file": file_path, "error": str(e)})
        return False
    except Exception as e:
        log_action("Unexpected error", {"file": file_path, "error": str(e)})
        return False