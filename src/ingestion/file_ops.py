import os
import pandas as pd
from datetime import datetime
from src.utils.logger import logger
from src.utils.utils import find_id_column
from src.utils.logger import logger
from src.utils.config_loader import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.db.db_ops import Clients
from collections import Counter
from src.db.db_ops import ProcessingJob
from src.utils.utils import find_id_column, is_valid_israeli_id


def ensure_filename_suffix(file_path):
    filename = os.path.basename(file_path)
    if '_' not in filename or not filename.split('_')[-1].split('.')[0].replace('-', '').isdigit():
        suffix = datetime.now().strftime('_%m%Y')
        new_filename = filename.split('.')[0] + suffix + '.' + filename.split('.')[-1]
        new_path = os.path.join(os.path.dirname(file_path), new_filename)
        os.rename(file_path, new_path)
        return new_path
    return file_path



def rename_file_with_id_column(file_path, session, job_id):
    try:
        df = pd.read_excel(file_path) if file_path.endswith('.xlsx') else pd.read_csv(file_path)
        sample_size = Config.get('validation.id_sample_size', 5)
        id_column = find_id_column(df, sample_size)
        if not id_column:
            logger.warning(f"No valid ID column found in {file_path}")
            move_file(file_path, 'data/failed', suffix='failed')
            session.query(ProcessingJob).filter_by(job_id=job_id).update({
                'status': 'FAILED',
                'error_summary': 'No valid ID column found',
                'finished_at': datetime.utcnow()
            })
            session.commit()
            return None
        
        sample = df[id_column].dropna().sample(min(sample_size, len(df[id_column].dropna())), random_state=42).tolist()
        valid_ids = [str(val).zfill(9) for val in sample if str(val).replace('-', '').isdigit() and is_valid_israeli_id(val)]
        
        db_config = Config.get('db', {})
        connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(connection_string)
        with sessionmaker(bind=engine)() as temp_session:
            clients = temp_session.query(Clients.client_id, Clients.provider).filter(Clients.client_id.in_(valid_ids)).all()
            found_ids = {c.client_id: c.provider for c in clients}
            missing_ids = len(valid_ids) - len(found_ids)
            
            if missing_ids > 1:
                logger.warning(f"More than one ID not found in clients table for {file_path}")
                new_path = move_file(file_path, 'data/failed', suffix='failed')
                session.query(ProcessingJob).filter_by(job_id=job_id).update({
                    'status': 'FAILED',
                    'error_summary': f'{missing_ids} IDs not found in clients table',
                    'finished_at': datetime.utcnow()
                })
                session.commit()
                return None, None
            
            providers = [found_ids.get(id) for id in valid_ids if id in found_ids]
            provider_counts = Counter(providers)
            majority_provider = provider_counts.most_common(1)[0][0] if provider_counts else None
            
            if majority_provider:
                filename = os.path.basename(file_path)
                base, ext = os.path.splitext(filename)
                suffix = datetime.now().strftime('_%m%Y')
                new_filename = f"{majority_provider}{suffix}{ext}"
                new_path = os.path.join(os.path.dirname(file_path), new_filename)
                os.rename(file_path, new_path)
                logger.info(f"Renamed {file_path} to {new_path} with provider {majority_provider}")
                return new_path, majority_provider
            else:
                logger.warning(f"No majority provider found for {file_path}")
                new_path = move_file(file_path, 'data/failed', suffix='failed')
                session.query(ProcessingJob).filter_by(job_id=job_id).update({
                    'status': 'FAILED',
                    'error_summary': 'No majority provider found',
                    'finished_at': datetime.utcnow()
                })
                session.commit()
                return None, None
            
    except Exception as e:
        logger.error(f"Failed to process {file_path}", extra={"error": str(e)})
        move_file(file_path, 'data/failed', suffix='failed')
        session.query(ProcessingJob).filter_by(job_id=job_id).update({
            'status': 'FAILED',
            'error_summary': str(e),
            'finished_at': datetime.utcnow()
        })
        session.commit()
        return None

def move_file(file_path, target_dir, suffix=None):
    """Move file to target directory, avoiding duplicates by adding timestamp."""
    os.makedirs(target_dir, exist_ok=True)
    base, ext = os.path.splitext(os.path.basename(file_path))
    if suffix:
        new_name = f"{base}_{suffix}{ext}"
    else:
        new_name = os.path.basename(file_path)
    target_path = os.path.join(target_dir, new_name)
    
    # Handle duplicates by appending timestamp
    counter = 1
    while os.path.exists(target_path):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        new_name = f"{base}_{suffix}_{timestamp}_{counter}{ext}" if suffix else f"{base}_{timestamp}_{counter}{ext}"
        target_path = os.path.join(target_dir, new_name)
        counter += 1
    
    try:
        os.rename(file_path, target_path)
        logger.info("Moved file", extra={"original": file_path, "target": target_path})
        return target_path
    except OSError as e:
        logger.error("Failed to move file", extra={"file": file_path, "target": target_path, "error": str(e)})
        raise