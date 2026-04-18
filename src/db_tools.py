from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.config import DB_DRIVER, DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def get_db_engine() -> Engine:
    """
    Creates and returns a database engine for connecting to SQL server. 
    Enabled fast_executemany options speeds up the insertion of large dataframes.

    Returns:
        Engine: SQLAlchemy engine instance ready to connect to the SQL Server database.

    Raises:
        Exception: If SQLAlchemy engine fails to initialize
    """
    connection_string = (
        f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        f"?driver={DB_DRIVER}&TrustServerCertificate=yes"
    )

    try:
        engine = create_engine(connection_string, fast_executemany=True)
        return engine
    except Exception as e:
        logger.error(f"Error during SQLAlchemy engine initialization: {e}")
        raise