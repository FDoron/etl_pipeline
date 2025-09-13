from pandas import DataFrame
import pandas as pd
from datetime import datetime
from src.db.db_ops import log_failed_row
from src.utils.utils import find_id_column, is_valid_israeli_id
from src.utils.config_loader import Config
from src.utils.logger import logger
from sqlalchemy.orm import sessionmaker

def normalize_and_validate(df: DataFrame, provider_mapping: dict, session, job_id: int):
    """Validate and transform dataframe, logging failed rows and ID validation."""
    column_mapping = provider_mapping.get('column_mapping', {})
    mandatory_columns = ['id', 'fee']
    required_columns = Config.get('required_columns', ['id', 'first_name', 'last_name', 'fee'])
    
    # Identify ID column using clients table
    sample_size = Config.get('validation.id_sample_size', 5)
    logger.debug(f"Identifying ID column with sample size {sample_size} for job_id={job_id}")
    id_column = find_id_column(df, sample_size)
    if not id_column:
        logger.error(f"No valid ID column found in clients table for job_id={job_id}")
        return None, {'error': 'No column with valid Israeli IDs found in clients table'}
    
    logger.info(f"Identified ID column: {id_column} for job_id={job_id}")
    
    # Update column mapping for all required fields
    temp_mapping = {id_column: 'id'}
    temp_mapping.update({k: v for k, v in column_mapping.items() if v in ['customer_name', 'fees']})
    # Map 'fees' to 'fee' and 'customer_name' to 'name' early
    temp_mapping = {k: 'name' if v == 'customer_name' else 'fee' if v == 'fees' else v for k, v in temp_mapping.items()}
    df = df.rename(columns=temp_mapping)
    # Check missing mandatory columns
    missing_cols = [col for col in mandatory_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing columns: {missing_cols} for job_id={job_id}")
        return None, {'error': f"Missing columns: {missing_cols}"}
    
    valid_rows = []
    errors = []
    
    # Validate each row
    for idx, row in df.iterrows():
        row_errors = []
        
        # Check for empty mandatory values
        for col in mandatory_columns:
            if pd.isna(row.get(col)):
                row_errors.append(f"{col} is missing")
        
        # Check data types and constraints for fee
        if not pd.isna(row.get('fee')):
            try:
                fee = float(row['fee'])
                if fee > 999:
                    row_errors.append(f"Fee {fee} exceeds maximum 999")
            except (ValueError, TypeError):
                row_errors.append("Fee is not numeric")
        
        # Validate ID against clients table
        if not pd.isna(row.get('id')):
            id_str = str(row['id']).zfill(9)
            if not is_valid_israeli_id(id_str):
                row_errors.append("Invalid Israeli ID")
            else:
                with sessionmaker(bind=session.bind)() as temp_session:
                    from src.db.db_ops import Clients
                    client = temp_session.query(Clients).filter_by(client_id=id_str).first()
                    if not client:
                        row_errors.append("ID not found in clients table")
                    else:
                        row['id'] = id_str
                        row['first_name'] = client.first_name
                        row['last_name'] = client.last_name
                        
        # Enrich with first_name and last_name from clients
        with sessionmaker(bind=session.bind)() as temp_session:
            from src.db.db_ops import Clients
            client = temp_session.query(Clients).filter_by(client_id=id_str).first()
            if client:
                row['first_name'] = client.first_name
                row['last_name'] = client.last_name
            else:
                row['first_name'] = None
                row['last_name'] = None
                
        # Add provider and reportPeriod
        provider = provider_mapping.get('provider', 'unknown')
        report_period = datetime.now().strftime('%Y-%m')
        row['provider'] = provider
        row['reportPeriod'] = report_period
        
        if row_errors:
            errors.append({'row': idx + 2, 'errors': row_errors})
            log_failed_row(session, job_id, idx + 2, row_errors)
            logger.warning(f"Row {idx + 2} failed validation for job_id={job_id}", extra={"errors": row_errors})
        else:
            valid_rows.append(row)
    
    if not valid_rows:
        logger.error(f"No valid rows after validation for job_id={job_id}")
        return None, {'error': 'No valid rows after validation'}
    
    # Create validated dataframe
    valid_df = pd.DataFrame(valid_rows)
    valid_df = valid_df.rename(columns={'customer_id': 'id', 'fees': 'fee'})
    valid_df = valid_df[[col for col in required_columns + ['provider', 'reportPeriod', 'status', 'job_id', 'ingested_at'] if col in valid_df.columns]]    
    # Add ingestion timestamp and job_id
    valid_df['ingested_at'] = datetime.now()
    valid_df['job_id'] = job_id
    valid_df['status'] = 'PROCESSED'
    
    logger.info(f"Validated {len(valid_rows)} rows for job_id={job_id}")
    return valid_df, errors