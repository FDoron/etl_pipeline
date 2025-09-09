import pandas as pd
from datetime import datetime

def normalize_and_validate(df: pd.DataFrame, required_columns: list, column_mapping: dict):
    """
    - Rename columns using mapping
    - Keep only required columns
    - customer_id as 9-digit string
    - Detect missing mandatory values, empty rows, invalid types
    - Fee > 999 fails
    Returns: df, valid (bool), errors (list)
    """
    errors = []

    # Detect unnamed numeric columns
    unnamed_numeric_cols = [c for c in df.columns if str(c).isdigit() or str(c).strip() == ""]
    if unnamed_numeric_cols:
        errors.append(f"Unnamed numeric columns detected: {unnamed_numeric_cols}")

    # Rename columns
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    # Check missing required columns
    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")

    # Keep only required columns
    df = df[required_columns] if all(c in df.columns for c in required_columns) else df

    # Detect empty rows or missing mandatory values
    for idx, row in df.iterrows():
        if row.isnull().all():
            errors.append(f"Row {idx} is completely empty")
        for col in required_columns:
            if pd.isna(row[col]):
                errors.append(f"Row {idx}: missing value in {col}")

    # customer_id as 9-digit string
    if "customer_id" in df.columns:
        try:
            df["customer_id"] = df["customer_id"].astype(str).str.zfill(9)
        except Exception as e:
            errors.append(f"Failed converting customer_id: {e}")

    # Fee validation
    if "fee" in df.columns:
        invalid_fee = df["fee"].apply(lambda x: not isinstance(x, (int, float)) or x > 999)
        if invalid_fee.any():
            errors.append(f"Rows with invalid fee (>999 or non-numeric): {df[invalid_fee].index.tolist()}")

    # Data type validation for other columns if needed
    # Example: customer_name as string
    if "customer_name" in df.columns:
        non_str = df["customer_name"].apply(lambda x: not isinstance(x, str))
        if non_str.any():
            errors.append(f"Rows with non-string customer_name: {df[non_str].index.tolist()}")

    valid = len(errors) == 0
    return df, valid, errors
