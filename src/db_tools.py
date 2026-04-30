"""
Database connectivity tools for the Air Quality Data Warehouse.

This module provides functions to establish a robust connection to the 
MS SQL Server database using SQLAlchemy and pyodbc. It acts as the bridge 
between the Python ETL scripts (Pandas) and the actual database.
"""

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.config import DB_DRIVER, DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy database engine for connecting to the SQL Server. 
    
    This function utilizes the 'fast_executemany' option, which significantly 
    speeds up the bulk insertion of large Pandas DataFrames during the Load phase.

    Returns:
        Engine: A SQLAlchemy engine instance ready to execute SQL queries.

    Raises:
        Exception: If the SQLAlchemy engine fails to initialize due to connection 
                   issues or incorrect credentials.
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