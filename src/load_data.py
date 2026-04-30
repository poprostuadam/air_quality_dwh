"""
Module responsible for the 'Load' phase of the ETL process.

It safely inserts transformed Pandas DataFrames into the MS SQL Server 
Data Warehouse tables (Facts and Dimensions) using SQLAlchemy. It utilizes 
the secure configuration from config.py and the database engine from db_tools.
"""
import pandas as pd
from loguru import logger

from src.db_tools import get_db_engine


def load_dimension_table(df: pd.DataFrame, engine):
    """
    Loads station data into the Dim_Station dimension table.

    Args:
        df (pd.DataFrame): The transformed DataFrame containing station data.
        engine: The SQLAlchemy engine instance connected to the database.

    Raises:
        Exception: If the SQL insert operation fails.
    """
    if df.empty:
        logger.warning("Brak danych stacji do załadowania.")
        return

    logger.info(f"Ładowanie {len(df)} wierszy do tabeli [Dim_Station]...")
    try:
        df.to_sql('Dim_Station', con=engine, if_exists='append', index=False)
        logger.success("Sukces: Załadowano Dim_Station.")
    except Exception as e:
        logger.error(f"Błąd ładowania Dim_Station: {e}")
        raise

def load_fact_table(df: pd.DataFrame, engine):
    """
    Loads air quality measurements into the Fact_AirQuality table.

    Args:
        df (pd.DataFrame): The transformed DataFrame containing measurement facts.
        engine: The SQLAlchemy engine instance connected to the database.

    Raises:
        Exception: If the SQL insert operation fails.
    """
    if df.empty:
        logger.warning("Brak danych pomiarowych do załadowania.")
        return

    logger.info(f"Ładowanie {len(df)} wierszy do tabeli [Fact_AirQuality]...")
    try:
        df.to_sql('Fact_AirQuality', con=engine, if_exists='append', index=False)
        logger.success("Sukces: Załadowano Fact_AirQuality.")
    except Exception as e:
        logger.error(f"Błąd ładowania Fact_AirQuality: {e}")
        raise

if __name__ == "__main__":
    logger.info("Testowanie połączenia z bazą danych przez db_tool...")
    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            logger.success("Połączenie nawiązane pomyślnie! db_tool i config.py działają idealnie.")
    except Exception as e:
        logger.error(f"Błąd połączenia. Sprawdź kontener Docker i plik .env. Szczegóły: {e}")
        raise