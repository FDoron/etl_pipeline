import pandas as pd
from datetime import datetime
from src.db.db_ops import log_failed_row


def normalize_and_validate(df, provider_mapping, session, job_id):
    """Validate and transform dataframe, logging failed rows."""
    mandatory_columns = provider_mapping.get('mandatory_columns', ['id', 'customer_name', 'fee'])
    required_columns = provider_mapping.get('required_columns', ['id', 'customer_name', 'fee'])
    column_mapping = provider_mapping.get('column_mapping', {})
    
    errors = []
    valid_rows = []
    
    # Check missing mandatory columns
    missing_cols = [col for col in mandatory_columns if col not in df.columns]
    if missing_cols:
        return None, {'error': f"Missing columns: {missing_cols}"}
    
    # Validate each row
    for idx, row in df.iterrows():
        row_errors = []
        
        # Check for empty mandatory values
        for col in mandatory_columns:
            if pd.isna(row.get(col)):
                row_errors.append(f"{col} is missing")
        
        # Check data types and constraints
        if not pd.isna(row.get('fee')):
            try:
                fee = float(row['fee'])
                if fee > 999:
                    row_errors.append(f"Fee {fee} exceeds maximum 999")
            except (ValueError, TypeError):
                row_errors.append("Fee is not numeric")
        
        if not pd.isna(row.get('id')):
            try:
                row['id'] = str(row['id']).zfill(9)  # Pad to 9 digits
            except (ValueError, TypeError):
                row_errors.append("ID cannot be converted to string")
        
        if row_errors:
            errors.append({'row': idx + 2, 'errors': row_errors})  # +2 for header and 0-based index
            log_failed_row(session, job_id, idx + 2, row_errors)
        else:
            valid_rows.append(row)
    
    if not valid_rows:
        return None, {'error': 'No valid rows after validation'}
    
    # Create validated dataframe
    valid_df = pd.DataFrame(valid_rows)
    
    # Apply column mapping and drop unmapped columns
    valid_df = valid_df.rename(columns=column_mapping)
    valid_df = valid_df[[col for col in required_columns if col in valid_df.columns]]
    
    # Add ingestion timestamp
    valid_df['ingestion_timestamp'] = datetime.now()
    
    return valid_df, errors
