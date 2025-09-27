from pandas import DataFrame
import pandas as pd
from datetime import datetime
from src.db.db_ops import log_failed_row
from src.utils.utils import find_id_column, is_valid_israeli_id
from src.utils.config_loader import Config
from src.utils.logger import logger
from sqlalchemy.orm import sessionmaker


def normalize_and_validate(df: DataFrame, provider_mapping: dict, session, job_id: int):
    """Validate mandatory columns from config, check for NaN values, and order columns."""
    # Get mandatory columns from config
    config_data = Config.get('data_prep', {})
    mandatory_columns = config_data.get('mandatory_columns', [])
    
    if not mandatory_columns:
        reason = "Missing mandatory_columns in configuration"
        logger.error(f"{reason} for job_id={job_id}")
        return None, {'status': 'failed', 'reason': reason}
    
    logger.debug(f"Starting normalization and validation for job_id={job_id}, columns={list(df.columns)}, mandatory={mandatory_columns}")
    
    # Check for mandatory columns
    missing_cols = [col for col in mandatory_columns if col not in df.columns]
    if missing_cols:
        reason = f"Missing mandatory columns: {missing_cols}"
        logger.error(f"{reason} for job_id={job_id}")
        return None, {'status': 'failed', 'reason': reason}
    
    # Check for NaN values in mandatory columns
    for col in mandatory_columns:
        if df[col].isna().any():
            reason = f"NaN values found in mandatory column: {col}"
            logger.error(f"{reason} for job_id={job_id}")
            return None, {'status': 'failed', 'reason': reason}
    
    # Order columns using mandatory_columns
    available_columns = [col for col in mandatory_columns if col in df.columns]
    
    # Create validated DataFrame with ordered columns
    valid_df = df[available_columns].copy()
    
    # Add provider and paid_month if missing and in mandatory_columns
    if 'provider' in mandatory_columns and 'provider' not in valid_df.columns:
        valid_df['provider'] = provider_mapping.get('provider', 'unknown')
    if 'paid_month' in mandatory_columns and 'paid_month' not in valid_df.columns:
        valid_df['paid_month'] = datetime.now().strftime('%m-%Y')  # Matches _find_date_step format
    
    # Row-by-row validation
    row_validation_rules = config_data.get('row_validation_rules', {})
    fee_values = row_validation_rules.get('fee_values', [])
    provider_reject = row_validation_rules.get('provider_reject', [])
    
    valid_rows = []
    invalid_rows = []
    
    for idx, row in df.iterrows():
        row_errors = []
        # Check for missing fee
        if pd.isna(row['fee']):
            row_errors.append("Missing fee value")
        # Check for invalid fee value
        elif fee_values and row['fee'] not in [float(v) for v in fee_values]:
            row_errors.append(f"Fee value {row['fee']} not in {fee_values}")
        # Check for unknown provider
        if row['provider'].lower() in [p.lower() for p in provider_reject]:
            row_errors.append(f"Provider {row['provider']} is unknown")
        
        if row_errors:
            # invalid_rows.append({'row_index': idx, 'errors': row_errors})
            invalid_rows.append({'row_index': idx, 'errors': row_errors, 'data': row.to_dict()})
        else:
            valid_rows.append(row)
    
    # Check if any valid rows exist
    if not valid_rows:
        reason = "No valid rows after row validation"
        logger.error(f"{reason} for job_id={job_id}")
        return None, {'status': 'failed', 'reason': reason}
    
    # Create validated DataFrame with ordered columns
    valid_df = pd.DataFrame(valid_rows)[mandatory_columns].copy()
    
    # Add ingestion metadata
    valid_df['ingested_at'] = datetime.now()
    valid_df['job_id'] = job_id
    valid_df['status'] = 'PROCESSED'
    
    logger.info(f"Validated DataFrame with {len(valid_df)} rows, columns={list(valid_df.columns)}, invalid_rows={len(invalid_rows)} for job_id={job_id}")
    return valid_df, {'status': 'ok', 'reason': 'Validation and normalization completed', 'invalid_rows': invalid_rows}