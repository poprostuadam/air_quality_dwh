"""
Moduł odpowiedzialny za ładowanie (Load) danych do bazy MS SQL Server.
Wykorzystuje bezpieczną konfigurację z config.py oraz narzędzie db_tool.
"""
import pandas as pd
from loguru import logger

from src.db_tools import get_db_engine


def load_dimension_table(df: pd.DataFrame, engine):
    """Ładuje dane stacji do tabeli Dim_Station."""
    if df.empty:
        logger.warning("Brak danych stacji do załadowania.")
        return

    logger.info(f"Ładowanie {len(df)} wierszy do tabeli [Dim_Station]...")
    try:
        df.to_sql('Dim_Station', con=engine, if_exists='append', index=False)
        logger.success("Sukces: Załadowano Dim_Station.")
    except Exception as e:
        logger.error(f"Błąd ładowania Dim_Station: {e}")

def load_fact_table(df: pd.DataFrame, engine):
    """Ładuje pomiary do tabeli Fact_AirQuality."""
    if df.empty:
        logger.warning("Brak danych pomiarowych do załadowania.")
        return

    logger.info(f"Ładowanie {len(df)} wierszy do tabeli [Fact_AirQuality]...")
    try:
        df.to_sql('Fact_AirQuality', con=engine, if_exists='append', index=False)
        logger.success("Sukces: Załadowano Fact_AirQuality.")
    except Exception as e:
        logger.error(f"Błąd ładowania Fact_AirQuality: {e}")

if __name__ == "__main__":
    logger.info("Testowanie połączenia z bazą danych przez db_tool...")
    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            logger.success("Połączenie nawiązane pomyślnie! db_tool i config.py działają idealnie.")
    except Exception as e:
        logger.error(f"Błąd połączenia. Sprawdź kontener Docker i plik .env. Szczegóły: {e}")