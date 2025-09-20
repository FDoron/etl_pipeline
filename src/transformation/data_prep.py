import pandas as pd
from datetime import datetime
import re
import os
from collections import Counter
from src.utils.config_loader import Config
from src.utils.logger import logger
from src.utils.utils import find_id_column, is_valid_israeli_id
from src.db.db_ops import Clients
from sqlalchemy.orm import sessionmaker
from charset_normalizer import detect


def prepare_data(df, provider_mapping, session, job_id):
    config = Config.get('data_prep', {})
    steps = config.get('steps', [])
    managed_count = 0

    # Map step names to functions
    step_functions = {
        'isolate_table': _isolate_table_step,
        'identify_provider': _identify_provider_step,
        'identify_id': _identify_id_step,
        'identify_fee': _identify_fee_step,
        'find_date': _find_date_step
    }

    for step in steps:
        if not config.get(f'{step}.enabled', True):
            logger.info(f"Step {step} disabled for job_id={job_id}")
            continue
        func = step_functions.get(step)
        if not func:
            logger.error(f"Step {step} not implemented for job_id={job_id}")
            return {'status': 'failed', 'df': None, 'reason': f'Step {step} not implemented'}
        status, df, reason = func(df, provider_mapping, session, job_id, config)
        logger.info(f"Step {step}: status={status}, reason={reason}, shape={df.shape if df is not None else None}", extra={"job_id": job_id})
        if status == 'failed':
            return {'status': 'failed', 'df': None, 'reason': reason}
        if status == 'managed':
            managed_count += 1
            if managed_count > config.get('issue_threshold', 1):
                return {'status': 'failed', 'df': None, 'reason': 'Too many managed issues'}
    return {'status': 'ok', 'df': df, 'reason': 'All steps completed'}

def _isolate_table_step(df, provider_mapping, session, job_id, config):
    df = df.dropna(how='all').dropna(axis=1, how='all')
    if not df.iloc[0].str.contains('מס_לקוח|תז|שם|דמי|סכום', na=False, regex=True).any():
        logger.warning(f"No recognizable headers in file for job_id={job_id}")
        df.columns = [f'col_{i}' for i in range(len(df.columns))]
        status = 'managed'
    else:
        status = 'ok'
    # Check for sporadic values
    non_mapped_cols = [col for col in df.columns if col not in provider_mapping.get('column_mapping', {}).keys()]
    sporadic_rows = df[non_mapped_cols].isna().sum(axis=1) > len(non_mapped_cols) * 0.5
    if sporadic_rows.sum() > 1:
        logger.error(f"Sporadic values detected in {sporadic_rows.sum()} rows for job_id={job_id}")
        return 'failed', None, 'Sporadic values in multiple rows'
    #TEMP
    logger.debug(f"After isolate_table: shape={df.shape}, columns={list(df.columns)}, dtypes={df.dtypes.to_dict()}")
    # for col in df.columns:
    #     logger.debug(f"Column {col} values: {df[col].tolist()}")
    return status, df, 'Table isolated successfully'

def _identify_id_step(df, provider_mapping, session, job_id, config):
    sample_size = min(config.get('validation.id_sample_size', 5), len(df))
    id_column = None
    for col in df.columns:
        sample = df[col].dropna().sample(n=min(sample_size, len(df[col].dropna())), random_state=42).astype(str)
        valid_count = 0
        # for val in sample:
        #     if re.match(r'^\d{9}$', val):
        #         if is_valid_israeli_id(val):
        #             valid_count += 1
        #     elif re.match(r'^\d{8}-\d{1}$', val):
        #         cleaned_val = val.replace('-', '')
        #         if is_valid_israeli_id(cleaned_val):
        #             valid_count += 1
        #             df[col] = df[col].astype(str).str.replace('-', '', regex=True)

        for val in sample:
            if re.match(r'^\d{7,9}$', val):
                padded_val = val.zfill(9)
                if is_valid_israeli_id(padded_val):
                    valid_count += 1
                    df[col] = df[col].astype(str).apply(lambda x: x.zfill(9) if re.match(r'^\d{7,9}$', x) else x)
                else:
                    logger.debug(f"Invalid ID in {col}: {padded_val} (raw: {val})", extra={"job_id": job_id})
            elif re.match(r'^\d{8}-\d{1}$', val):
                cleaned_val = val.replace('-', '')
                if is_valid_israeli_id(cleaned_val):
                    valid_count += 1
                    df[col] = df[col].astype(str).str.replace('-', '', regex=True)
                else:
                    logger.debug(f"Invalid hyphenated ID in {col}: {cleaned_val} (raw: {val})", extra={"job_id": job_id})
            else:
                logger.debug(f"Non-numeric or malformed ID in {col}: {val}", extra={"job_id": job_id})
        if valid_count == len(sample):
            id_column = col
            break
    if not id_column:
        logger.error(f"No valid ID column found for job_id={job_id}")
        return 'failed', None, 'No valid ID column found'
    df = df.rename(columns={id_column: 'id'})
    return 'ok', df, f'ID column identified: {id_column}'

def _identify_fee_step(df, provider_mapping, session, job_id, config):
    sample_size = min(config.get('validation.id_sample_size', 5), len(df))
    fee_values = config.get('fee_values', [62, 30])
    fee_outlier_threshold = config.get('fee_outlier_threshold', 10)
    fee_column = None
    clean_df = df.copy()
    problematic_rows = None
    
    logger.debug(f"Starting fee identification: columns={list(df.columns)}, dtypes={df.dtypes.to_dict()}, sample_size={sample_size}, fee_values={fee_values}", extra={"job_id": job_id})
    
    for col in df.columns:
        sample_numeric = pd.to_numeric(df[col].dropna(), errors='coerce')
        if sample_numeric.isna().sum() > 0:
            logger.debug(f"Column {col} has {sample_numeric.isna().sum()} non-numeric values: {df[col].dropna().tolist()[:5]}", extra={"job_id": job_id})
            continue
        sample = sample_numeric.sample(n=min(sample_size, len(sample_numeric.dropna())), random_state=42)
        logger.debug(f"Column {col} sample: {sample.tolist()}, dtype: {sample.dtype}", extra={"job_id": job_id})
        
        # Count outliers (NaN or not in fee_values)
        outliers = df[col].isna() | ~df[col].astype(str).isin([str(v) for v in fee_values])
        outlier_count = outliers.sum()
        logger.debug(f"Column {col} has {outlier_count} outliers (not in {fee_values} or empty)", extra={"job_id": job_id})
        
        if outlier_count > fee_outlier_threshold:
            logger.debug(f"Skipping {col}: too many outliers ({outlier_count} > {fee_outlier_threshold})", extra={"job_id": job_id})
            continue
            
        if outlier_count <= fee_outlier_threshold:
            fee_column = col
            if outlier_count > 0:
                problematic_rows = df[outliers].copy()
                clean_df = df[~outliers].copy()
                provider = clean_df.get('provider', clean_df.get('col_4', pd.Series())).iloc[0] if ('provider' in clean_df.columns or 'col_4' in clean_df.columns) else 'unknown'
                manual_file = f"data/processed/iutuit5_manual_{provider}_092025.xlsx"
                problematic_rows.to_excel(manual_file, index=False)
                logger.info(f"Saved {outlier_count} problematic rows to {manual_file}", extra={"job_id": job_id})
            break
    
    if not fee_column:
        logger.error(f"No valid fee column found for job_id={job_id}", extra={"job_id": job_id})
        return 'managed', clean_df, 'No valid fee column found'
    
    clean_df = clean_df.rename(columns={fee_column: 'fee'})
    logger.info(f"Fee column identified: {fee_column}, {outlier_count} outliers handled", extra={"job_id": job_id})
    return 'ok', clean_df, f'Fee column identified: {fee_column}'

def _find_date_step(df, provider_mapping, session, job_id, config):
    date_formats = config.get('date_formats', ['%d-%m-%Y', '%d/%m/%Y', '%d%m%Y', '%m-%d-%Y', '%m/%d/%Y', '%b-%Y'])
    sample_size = min(config.get('validation.id_sample_size', 5), len(df))
    current_date = datetime.now()
    cutoff_day = config.get('cutoff_day', 5)
    target_month = current_date.strftime('%m-%Y') if current_date.day >= cutoff_day else (current_date.replace(month=current_date.month - 1)).strftime('%m-%Y')
    last_three_months = [(current_date - pd.offsets.MonthBegin(n)).strftime('%m-%Y') for n in range(3)]
    
    logger.debug(f"Starting date identification: columns={list(df.columns)}, dtypes={df.dtypes.to_dict()}, sample_size={sample_size}, last_three_months={last_three_months}, target_month={target_month}", extra={"job_id": job_id})
    
    date_column = None
    for col in df.columns:
        sample = df[col].dropna().sample(n=min(sample_size, len(df[col].dropna())), random_state=42).astype(str)
        logger.debug(f"Column {col} sample: {sample.tolist()}", extra={"job_id": job_id})
        parsed_dates = []
        for val in sample:
            for fmt in date_formats:
                try:
                    parsed = pd.to_datetime(val, format=fmt, errors='coerce')
                    if not pd.isna(parsed) and parsed.strftime('%m-%Y') in last_three_months:
                        parsed_dates.append(parsed)
                        break
                except (ValueError, TypeError):
                    continue
        if len(parsed_dates) / len(sample) > 0.7:
            date_column = col
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%m-%Y')
            break
    
    if date_column:
        df = df.rename(columns={date_column: 'paid_month'})
        logger.info(f"Date column identified: {date_column}, formatted as mm-YYYY", extra={"job_id": job_id})
        return 'ok', df, f'Date column identified: {date_column}'
    
    logger.warning(f"No date column found, setting paid_month to {target_month}", extra={"job_id": job_id})
    df['paid_month'] = target_month
    return 'managed', df, f'No date column, set to {target_month}'

def _identify_provider_step(df, provider_mapping, session, job_id, config):
    providers = config.get('data_prep.providers', {})
    sample_size = min(config.get('validation.id_sample_size', 5), len(df))
    logger.debug(f"DataFrame shape: {df.shape}, columns: {list(df.columns)}, sample_size: {sample_size}")
    provider_column = None
    selected_provider = None
    logger.debug(f"Columns: {list(df.columns)}, col_4 exists: {'col_4' in df.columns}")
    non_numeric_cols = [col for col in df.columns if df[col].dtype == 'object']
    logger.debug(f"Non-numeric columns: {non_numeric_cols}, all columns: {list(df.columns)}")
    # Check encoding of text data
    text = ' '.join(df[non_numeric_cols].astype(str).values.flatten()).encode()
    encoding = detect(text).get('encoding', 'utf-8')
    logger.debug(f"Detected encoding after isolate_table: {encoding}")
    for col in non_numeric_cols or df.columns:
        non_null_count = len(df[col].replace(['', 'None'], pd.NA).dropna())
        logger.debug(f"Column {col} non-null count: {non_null_count}, values: {df[col].tolist()}")
        if non_null_count == 0:
            continue
        sample = df[col].replace(['', 'None'], pd.NA).dropna().sample(n=min(sample_size, non_null_count), random_state=42).astype(str)
        provider_counts = Counter(sample)
        majority_provider, count = provider_counts.most_common(1)[0] if provider_counts else (None, 0)
        if count / len(sample) > 0.8:  # Majority value is consistent
            provider_column = col
            selected_provider = majority_provider  # Use raw majority value
            providers = config.get('data_prep.providers', {})
            if majority_provider not in providers:
                logger.warning(f"Unregistered provider '{majority_provider}' in column {col}", extra={"job_id": job_id})
            else:
                selected_provider = providers.get(majority_provider, majority_provider)
            provider_mapping['provider'] = selected_provider
            break
    if provider_column:
        provider_mapping['provider'] = selected_provider
        logger.info(f"Provider column identified: {provider_column}, provider: {selected_provider}", extra={"job_id": job_id})
        return 'ok', df, f'Provider column identified: {provider_column}, provider: {selected_provider}'
    logger.info(f"No provider column found for job_id={job_id}")
    return 'managed', df, 'No provider column, will use ID-based provider identification'

def _find_date_step(df, provider_mapping, session, job_id, config):
    date_formats = config.get('date_formats', ['%d-%m-%Y', '%d/%m/%Y', '%Y%m%d', '%m-%d-%Y', '%m/%d/%Y', '%b-%Y'])
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year
    current_day = current_date.day
    cutoff_day = 5  # Configurable cutoff
    target_month = current_month if current_day >= cutoff_day else current_month - 1
    if target_month == 0:
        target_month = 12
        current_year -= 1
    target_date = datetime(current_year, target_month, 1)
    logger.debug(f"Target date: {target_date.strftime('%m-%Y')}, cutoff: day {cutoff_day}", extra={"job_id": job_id})
    
    sample_size = min(config.get('validation.id_sample_size', 5), len(df))
    date_column = None
    for col in df.columns:
        sample = df[col].dropna().sample(n=min(sample_size, len(df[col].dropna())), random_state=42).astype(str)
        parsed_dates = []
        for val in sample:
            for fmt in date_formats:
                try:
                    parsed = datetime.strptime(val, fmt)
                    if parsed.year in [current_year, current_year - 1] and parsed.month in [target_month, target_month - 1, target_month - 2]:
                        parsed_dates.append(parsed)
                        break
                except ValueError:
                    continue
        if len(parsed_dates) / len(sample) > 0.7:
            month_counts = Counter([d.month for d in parsed_dates])
            majority_month = month_counts.most_common(1)[0][1] / len(parsed_dates)
            if majority_month > 0.7:
                date_column = col
                break
    if date_column:
        df[date_column] = df[date_column].astype(str).apply(lambda x: datetime.strptime(x, next(fmt for fmt in date_formats if not pd.isna(datetime.strptime(x, fmt, errors='ignore')))).strftime('%m-%Y') if pd.notna(x) else None)
        logger.info(f"Date column identified: {date_column}", extra={"job_id": job_id})
        return 'ok', df, f'Date column identified: {date_column}'
    else:
        logger.info(f"No date column found, setting to {target_date.strftime('%m-%Y')}", extra={"job_id": job_id})
        df['paid_month'] = target_date.strftime('%m-%Y')
        return 'managed', df, 'No date column, set to current month'