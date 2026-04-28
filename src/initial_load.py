"""
Jednorazowy skrypt do pobrania danych historycznych (Initial Load).
Uruchamiamy go tylko raz, aby napełnić pustą bazę MS SQL.
"""
from loguru import logger

from src.api_client import fetch_locations, fetch_measurements
from src.db_tools import get_db_engine
from src.etl_measurements import transform_locations, transform_measurements
from src.load_data import load_dimension_table, load_fact_table


def run_historical_load():
    logger.info("Rozpoczynam historyczne zasilanie bazy (ostatnie 365 dni)...")
    engine = get_db_engine()
    
    # 1. Pobierzemy np. 30 głównych stacji z Polski
    poland_bbox = (14.12, 49.00, 24.15, 54.84)
    stations = fetch_locations(limit=50, bbox=poland_bbox)
    active_stations = [s for s in stations if s.get('datetime_last')]
    
    # 2. POBIERANIE WSTECZ: Ustawiamy days_history na 365 dni!
    raw_meas = fetch_measurements(locations_data=active_stations, days_history=365)
    
    # 3. Transform & Load
    df_stations = transform_locations(stations)
    df_meas = transform_measurements(raw_meas)
    
    load_dimension_table(df_stations, engine)
    load_fact_table(df_meas, engine)
    
    logger.success("Sukces! Baza została napełniona danymi historycznymi.")

if __name__ == "__main__":
    run_historical_load()