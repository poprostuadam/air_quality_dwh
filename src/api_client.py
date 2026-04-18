"""
Module responsible for fetching data from OpenAQ API v3.
Implements Pagination and strict parameter types according to official documentation.
"""
import time
from datetime import datetime, timedelta, timezone
from openaq import OpenAQ
from loguru import logger
from src.config import OPENAQ_API_KEY

# Initialize single client instance
client = OpenAQ(api_key=OPENAQ_API_KEY) if OPENAQ_API_KEY else OpenAQ()

def fetch_locations(limit: int = 100, bbox: tuple = None) -> list:
    """
    Fetches locations data and returns a list of dictionaries.
    """
    if bbox is None:
        bbox = (14.12, 49.00, 24.15, 54.84)
        
    logger.info(f"Fetching locations for bbox {bbox} (limit: {limit})...")
    try:
        response = client.locations.list(limit=limit, bbox=bbox)
        return response.dict().get('results', [])
    except Exception as e:
        logger.error(f"Error fetching locations: {e}")
        return []

def fetch_measurements(locations_data: list, target_params: list = [1, 2], days_history: int = 5) -> list:
    """
    Fetches FULL history for targeted sensors within 'days_history' window.
    Uses 'datetime_from' and 'data' parameters as required by API v3.
    """
    logger.info(f"Extracting sensors and fetching up to {days_history} days of history...")
    all_measurements = []
    
    # Format required by datetime_from
    dt_from = (datetime.now(timezone.utc) - timedelta(days=days_history)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # 1. Extract targeted sensors
    target_sensors = []
    for loc in locations_data:
        loc_id = loc.get('id')
        sensors = loc.get('sensors', [])
        
        for sensor in sensors:
            param_id = sensor.get('parameter', {}).get('id')
            if param_id in target_params:
                target_sensors.append((loc_id, sensor.get('id')))

    # 2. Fetch measurements securely using Pagination
    for idx, (loc_id, sensor_id) in enumerate(target_sensors, 1):
        logger.debug(f"[{idx}/{len(target_sensors)}] Fetching sensor {sensor_id} (Location: {loc_id}) since {dt_from}...")  # noqa: E501
        
        page = 1
        has_more_data = True
        sensor_records = 0
        
        while has_more_data:
            try:
                # FIXED: Added 'data' (required) and changed to 'datetime_from'
                response = client.measurements.list(
                    sensors_id=sensor_id,
                    data="measurements",     # The base measurement unit to query
                    datetime_from=dt_from,   # Correct param for data='measurements'
                    limit=1000,
                    page=page
                )
                
                results = response.dict().get('results', [])
                
                if not results:
                    has_more_data = False
                    break
                
                for r in results:
                    r['location_id'] = loc_id
                    all_measurements.append(r)
                    sensor_records += 1
                
                if len(results) < 1000:
                    has_more_data = False
                else:
                    page += 1
                    
            except Exception as e:
                logger.warning(f"Failed fetching page {page} for Sensor {sensor_id}: {e}")
                has_more_data = False
                
            # RATE LIMIT PROTECTION
            time.sleep(1.1)
            
        logger.debug(f"Finished sensor {sensor_id}: downloaded {sensor_records} total records across {page} pages.")

    logger.success(f"Aggregated {len(all_measurements)} total measurement readings.")
    return all_measurements

if __name__ == "__main__":
    poland_bbox = (14.12, 49.00, 24.15, 54.84)
    
    stations = fetch_locations(limit=15, bbox=poland_bbox)
    print(f"\n[TEST] Fetched {len(stations)} stations.")
    

    if stations:
        # Testing pagination with a 5-day history
        meas = fetch_measurements(locations_data=stations, target_params=[1, 2], days_history=5)
        if meas:
            print(f"\n[TEST] Total measurements fetched: {len(meas)}")
            print("[TEST] Sample Measurement Dict:")
            print(meas[-2:])
    print('\n')
    print(stations[-2:])