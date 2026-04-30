"""
Module responsible for transforming raw OpenAQ JSON data into Pandas DataFrames.

This module handles the 'Transform' phase of the ETL process. It flattens nested
JSON structures, filters necessary columns, renames them to match the SQL Star Schema,
cleans data types, and generates surrogate keys (like date_key) for the Data Warehouse.
"""
import pandas as pd
from loguru import logger
import src.api_client as api

def transform_locations(locations_data: list) -> pd.DataFrame:
    """
    Transforms raw locations data into the Dimension Table format (Dim_Station).

    Args:
        locations_data (list): A list of dictionaries containing raw location data from the API.

    Returns:
        pd.DataFrame: A formatted Pandas DataFrame ready for the Dim_Station table. 
                      Returns an empty DataFrame if the input list is empty.
    """
    if not locations_data:
        logger.warning("No locations data provided for transformation.")
        return pd.DataFrame()

    logger.info("Transforming locations data into DataFrame...")
    
    # 1. Flatten the nested JSON structure
    df = pd.json_normalize(locations_data)
    
    # 2. Select columns required for the database (Dim_Station)
    cols_to_keep = ['id', 'name', 'coordinates.latitude', 'coordinates.longitude', 'country.name']
    existing_cols = [c for c in cols_to_keep if c in df.columns]
    df = df[existing_cols]
    
    # 3. Rename columns to match SQL schema equivalents
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
    Transforms raw measurements data into the Fact Table format (Fact_AirQuality).

    Args:
        measurements_data (list): A list of dictionaries containing raw measurement data.

    Returns:
        pd.DataFrame: A formatted Pandas DataFrame ready for the Fact_AirQuality table.
                      Returns an empty DataFrame if the input list is empty.
    """
    if not measurements_data:
        logger.warning("No measurements data provided for transformation.")
        return pd.DataFrame()

    logger.info("Transforming measurements data into DataFrame...")
    
    # 1. Flatten the nested JSON structure
    df = pd.json_normalize(measurements_data)
    
    # 2. Identify the date column (OpenAQ v3 stores it specifically depending on the endpoint)
    dt_col = 'period.datetime_to.utc' if 'period.datetime_to.utc' in df.columns else 'datetime.utc'
    
    # 3. Select columns required for the database (Fact_AirQuality)
    cols_to_keep = ['location_id', 'parameter.id', 'value', dt_col]
    existing_cols = [c for c in cols_to_keep if c in df.columns]
    df = df[existing_cols]
    
    # 4. Rename columns to match SQL schema
    df.rename(columns={
        'parameter.id': 'parameter_id',
        dt_col: 'measured_at'
    }, inplace=True)
    
    # 5. Clean data types (crucial before SQL database insertion)
    if 'measured_at' in df.columns:
        df['measured_at'] = pd.to_datetime(df['measured_at'])
        
    if 'value' in df.columns:
        df = df.dropna(subset=['value'])
        
    logger.success(f"Measurements DataFrame ready. Shape: {df.shape}")

    # Generate a date key for the Dim_Date dimension (format YYYYMMDD as integer)
    df['date_key'] = df['measured_at'].dt.strftime('%Y%m%d').astype(int)
    
    logger.success(f"Measurements DataFrame ready. Shape: {df.shape}")
    return df

if __name__ == "__main__":
    from datetime import datetime, timedelta, timezone
    
    test_bbox = (14.12, 49.00, 24.15, 54.84)
    
    raw_stations = api.fetch_locations(limit=20, bbox=test_bbox)
    
    cutoff = datetime.now(timezone.utc) - timedelta(days=5)
    active_stations = [
        s for s in raw_stations 
        if s.get('datetime_last', {}).get('utc') and 
        datetime.strptime(s['datetime_last']['utc'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) >= cutoff
    ]
                
    raw_measurements = api.fetch_measurements(locations_data=active_stations, target_params=[1, 2], days_history=1)
    
    print("\n--- DIMENSION TABLE (STATIONS) ---")
    df_dim = transform_locations(raw_stations)
    print(df_dim.head().to_markdown(index=False) if not df_dim.empty else "No data") 
    
    print("\n--- FACT TABLE (MEASUREMENTS) ---")
    df_fact = transform_measurements(raw_measurements)
    print(df_fact.head().to_markdown(index=False) if not df_fact.empty else "No facts to display")