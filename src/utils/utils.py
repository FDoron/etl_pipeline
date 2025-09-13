
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.utils.config_loader import Config
from src.db.db_ops import Clients

def is_valid_israeli_id(id_str):
    try:
        id_str = str(id_str).zfill(9)
        if len(id_str) != 9 or not id_str.isdigit():
            return False
        total = 0
        for i, digit in enumerate(id_str):
            weight = 1 if i % 2 == 0 else 2
            product = int(digit) * weight
            total += product if product < 10 else product - 9
        return total % 10 == 0
    except (ValueError, TypeError):
        return False

def find_id_column(df, sample_size=5):
    for col in df.columns:
        sample = df[col].dropna().sample(min(sample_size, len(df[col].dropna())), random_state=42).tolist()
        valid_ids = [str(val).zfill(9) for val in sample if str(val).replace('-', '').isdigit() and is_valid_israeli_id(val)]
        if valid_ids and len(valid_ids) == len(sample):
            return col
    return None