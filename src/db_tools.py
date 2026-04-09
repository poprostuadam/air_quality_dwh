from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from loguru import logger

DB_USER="sa"
DB_PASSWORD="Password1234"
DB_HOST="localhost"
DB_PORT="1433"
DB_NAME="master"
DB_DRIVER = "ODBC+Driver+18+for+SQL+Server"

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