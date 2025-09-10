import pandas as pd
from datetime import datetime
from src.db.db_ops import log_failed_row

def normalize_and_validate(df, provider_mapping, session, job_id):
    """Validate and transform dataframe, logging failed rows."""
    column_mapping = provider_mapping.get('column_mapping', {})
    mandatory_columns = ['customer_id', 'customer_name', 'fees']
    required_columns = ['id', 'name', 'fee']
    
    # Rename columns before validation
    df = df.rename(columns=column_mapping)
    
    # Check missing mandatory columns
    missing_cols = [col for col in mandatory_columns if col not in df.columns]
    if missing_cols:
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
        
        # Check data types and constraints
        if not pd.isna(row.get('fees')):
            try:
                fee = float(row['fees'])
                if fee > 999:
                    row_errors.append(f"Fee {fee} exceeds maximum 999")
            except (ValueError, TypeError):
                row_errors.append("Fee is not numeric")
        
        if not pd.isna(row.get('customer_id')):
            try:
                row['customer_id'] = str(row['customer_id']).zfill(9)
            except (ValueError, TypeError):
                row_errors.append("ID cannot be converted to string")
        
        # Add provider and reportPeriod
        provider = provider_mapping.get('provider', 'unknown')
        report_period = datetime.now().strftime('%Y-%m')
        row['provider'] = provider
        row['reportPeriod'] = report_period
        
        if row_errors:
            errors.append({'row': idx + 2, 'errors': row_errors})
            log_failed_row(session, job_id, idx + 2, row_errors)
        else:
            valid_rows.append(row)
    
    if not valid_rows:
        return None, {'error': 'No valid rows after validation'}
    
    # Create validated dataframe
    valid_df = pd.DataFrame(valid_rows)
    
    # Apply column mapping and filter required columns
    valid_df = valid_df.rename(columns=column_mapping)
    valid_df = valid_df.rename(columns={'customer_id': 'id', 'customer_name': 'name', 'fees': 'fee'})
    valid_df = valid_df[[col for col in required_columns + ['provider', 'reportPeriod', 'status', 'job_id', 'ingested_at'] if col in valid_df.columns]]
    
    # Add ingestion timestamp and job_id
    valid_df['ingested_at'] = datetime.now()
    valid_df['job_id'] = job_id
    valid_df['status'] = 'PROCESSED'
    
    return valid_df, errors