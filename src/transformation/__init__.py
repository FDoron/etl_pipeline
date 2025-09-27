import pandas as pd
import json
import os

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "../../config/providers.json")

with open(CONFIG_FILE, "r") as f:
    PROVIDER_CONFIG = json.load(f)

# REQUIRED_COLUMNS = ["CustomerID", "CustomerName", "MonthlyFee", "ReportingMonth"]

def validate_and_clean(df, provider):
    mapping = PROVIDER_CONFIG.get(provider, {}).get("columns", {})

    # Rename columns
    col_map = {}
    for std_col, synonyms in mapping.items():
        for c in df.columns:
            if c.strip().lower() in [s.lower() for s in synonyms]:
                col_map[c] = std_col
    df = df.rename(columns=col_map)

    # Check required columns
    # missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    # if missing:
    #     raise ValueError(f"Missing required columns: {missing}")

    # Drop rows with null IDs or Fees
    df = df.dropna(subset=["CustomerID", "MonthlyFee"])

    # Cast types
    df["MonthlyFee"] = pd.to_numeric(df["MonthlyFee"], errors="coerce")
    df = df.dropna(subset=["MonthlyFee"])

    return df
