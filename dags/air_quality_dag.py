import sys
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, '/opt/airflow')
logger = logging.getLogger("airflow.task")

from src.api_client import fetch_locations, fetch_measurements
from src.db_tools import get_db_engine
from src.etl_measurements import transform_measurements
from src.load_data import load_fact_table

def run_full_etl_process():
    logger.info("Rozpoczynam docelowy proces ETL (Delta Load)...")
    engine = get_db_engine()
    
    # 1. EXTRACT
    poland_bbox = (14.12, 49.00, 24.15, 54.84)
    raw_stations = fetch_locations(limit=50, bbox=poland_bbox)
    active_stations = [s for s in raw_stations if s.get('datetime_last')]
    
    logger.info(f"Pobrano {len(active_stations)} aktywnych stacji. Zaczynam pobierać pomiary z ostatnich 24h...")
    raw_meas = fetch_measurements(locations_data=active_stations, days_history=1)
    
    # 2. TRANSFORM
    df_meas = transform_measurements(raw_meas)
    row_count = len(df_meas)
    
    if row_count == 0:
        logger.warning("Brak nowych danych z API za ostatnie 24h.")
        return

    logger.info(f"Transformacja udana. Ładowanie {row_count} wierszy do MS SQL...")
    
    # 3. LOAD
    load_fact_table(df_meas, engine)
    logger.info("Proces zakończony sukcesem!")

default_args = {
    'owner': 'adam',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'air_quality_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id='run_etl_task',
        python_callable=run_full_etl_process
    )