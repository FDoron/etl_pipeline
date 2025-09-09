import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError
from src.utils.config_loader import Config
from src.utils.logger import get_logger

logger = get_logger(__name__)

class Database:
    _engine = None

    @classmethod
    def get_engine(cls):
        if cls._engine is None:
            db_conf = Config.get("db")
            conn_str = f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}@" \
                       f"{db_conf['host']}:{db_conf['port']}/{db_conf['database']}"
            try:
                cls._engine = sa.create_engine(conn_str, pool_pre_ping=True)
                logger.info("Database engine created successfully.")
            except SQLAlchemyError as e:
                logger.error(f"Error creating DB engine: {e}")
                raise
        return cls._engine
