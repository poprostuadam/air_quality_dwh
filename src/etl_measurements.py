"""
Module responsible for Transforming raw OpenAQ dictionaries into Pandas DataFrames.
Flattens JSON structures to prepare them for Star Schema loading.
"""
import pandas as pd
from loguru import logger
import src.api_client as api

def transform_locations(locations_data: list) -> pd.DataFrame:
    """
    Transforms raw locations into the Dimension Table format (Dim_Station).
    """
    if not locations_data:
        logger.warning("No locations data provided for transformation.")
        return pd.DataFrame()

    logger.info("Transforming locations data into DataFrame...")
    
    # 1. Spłaszczenie JSON-a
    df = pd.json_normalize(locations_data)
    
    # 2. Wybór kolumn do bazy (Dim_Station)
    cols_to_keep = ['id', 'name', 'coordinates.latitude', 'coordinates.longitude', 'country.name']
    existing_cols = [c for c in cols_to_keep if c in df.columns]
    df = df[existing_cols]
    
    # 3. Zmiana nazw na czyste SQL-owe odpowiedniki
    df.rename(columns={
        'id': 'location_id',
        'name': 'station_name',
        'coordinates.latitude': 'latitude',
        'coordinates.longitude': 'longitude',
        'country.name': 'country'
    }, inplace=True)
    
    logger.success(f"Locations DataFrame ready. Shape: {df.shape}")
    return df

def transform_measurements(measurements_data: list) -> pd.DataFrame:
    """
    Transforms raw measurements into the Fact Table format (Fact_AirQuality).
    """
    if not measurements_data:
        logger.warning("No measurements data provided for transformation.")
        return pd.DataFrame()

    logger.info("Transforming measurements data into DataFrame...")
    
    # 1. Spłaszczenie JSON-a
    df = pd.json_normalize(measurements_data)
    
    # 2. Identyfikacja kolumny z datą (OpenAQ v3 przechowuje ją specyficznie)
    dt_col = 'period.datetime_to.utc' if 'period.datetime_to.utc' in df.columns else 'datetime.utc'
    
    # 3. Wybór kolumn do bazy (Fact_AirQuality) - uwaga na wstrzyknięty location_id
    cols_to_keep = ['location_id', 'parameter.id', 'value', dt_col]
    existing_cols = [c for c in cols_to_keep if c in df.columns]
    df = df[existing_cols]
    
    # 4. Zmiana nazw na SQL-owe
    df.rename(columns={
        'parameter.id': 'parameter_id',
        dt_col: 'measured_at'
    }, inplace=True)
    
    # 5. Czyszczenie typów danych (niezbędne przed bazą SQL)
    if 'measured_at' in df.columns:
        df['measured_at'] = pd.to_datetime(df['measured_at'])
        
    if 'value' in df.columns:
        # Usunięcie pustych odczytów
        df = df.dropna(subset=['value'])
        
    logger.success(f"Measurements DataFrame ready. Shape: {df.shape}")
    # Generowanie klucza daty pod wymiar Dim_Date (format YYYYMMDD jako INT)
    df['date_key'] = df['measured_at'].dt.strftime('%Y%m%d').astype(int)
    
    logger.success(f"Measurements DataFrame ready. Shape: {df.shape}")
    return df

if __name__ == "__main__":
    from datetime import datetime, timedelta, timezone
    
    # Szybki test całego łańcucha E -> T
    test_bbox = (14.12, 49.00, 24.15, 54.84)
    
    # [E] Pobieranie
    raw_stations = api.fetch_locations(limit=20, bbox=test_bbox)
    
    cutoff = datetime.now(timezone.utc) - timedelta(days=5)
    active_stations = [
        s for s in raw_stations 
        if s.get('datetime_last', {}).get('utc') and 
        datetime.strptime(s['datetime_last']['utc'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) >= cutoff
    ]
                
    raw_measurements = api.fetch_measurements(locations_data=active_stations, target_params=[1, 2], days_history=1)
    
    # [T] Transformacja
    print("\n--- DIMENSION TABLE (STATIONS) ---")
    df_dim = transform_locations(raw_stations)
    print(df_dim.head().to_markdown(index=False) if not df_dim.empty else "No data") 
    
    print("\n--- FACT TABLE (MEASUREMENTS) ---")
    df_fact = transform_measurements(raw_measurements)
    print(df_fact.head().to_markdown(index=False) if not df_fact.empty else "No facts to display")