import pandas as pd
import json
import os

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "../../config/providers.json")

with open(CONFIG_FILE, "r") as f:
    PROVIDER_CONFIG = json.load(f)

def detect_provider(file_name):
    for provider in PROVIDER_CONFIG:
        if file_name.lower().startswith(provider.lower()):
            return provider
    return "Unknown"

def ingest_file(file_path):
    file_name = os.path.basename(file_path)
    provider = detect_provider(file_name)

    if file_path.endswith((".xlsx", ".xls")):
        df = pd.read_excel(file_path)
    else:
        df = pd.read_csv(file_path)

    return df, provider
