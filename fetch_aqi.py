from openaq import OpenAQ
from openaq.shared.exceptions import ServerError
import time
import pandas as pd

# Initialize client
client = OpenAQ(api_key="9532f24560378a32472d71772950f5dd063cfdacbb769e00ea9d4ed05a93fa9a")

def safe_sensor_get(sensor_id, max_retries=3, retry_delay=2):
    """Helper function with retry logic for sensor data fetching"""
    for attempt in range(max_retries):
        try:
            return client.sensors.get(sensors_id=sensor_id)
        except ServerError as e:
            if attempt == max_retries - 1:
                print(f"⚠️ Failed to fetch sensor {sensor_id} after {max_retries} attempts")
                return None
            print(f"🔄 Retry {attempt + 1}/{max_retries} for sensor {sensor_id}")
            time.sleep(retry_delay)
    return None

import os
os.makedirs('data', exist_ok=True)  # Ensure directory exists

try:
    mumbai_bbox = [72.7759, 18.8935, 72.9762, 19.2550]
    
    # Fetch locations with retry logic
    locations = None
    for attempt in range(3):
        try:
            locations = client.locations.list(
                parameters_id=2,
                coordinates=f"{mumbai_bbox[1]},{mumbai_bbox[0]}",
                radius=25000,
                limit=10
            ).results
            break
        except ServerError:
            if attempt == 2:
                raise
            time.sleep(2)
    
    if not locations:
        print("❌ Failed to fetch locations data")
        exit(1)
    
    print(f"Found {len(locations)} locations")
    
    for loc in locations:
        print(f"\nProcessing location {loc.id} - {loc.name or 'Unnamed'}")
        
        if not hasattr(loc, 'sensors') or not loc.sensors:
            print("⚠️ No sensors found for this location")
            continue
            
        for s in loc.sensors:
            if s.parameter.id == 2 and s.id:  # PM2.5 sensors only
                print(f"\n🔍 Found PM2.5 sensor {s.id}")
                
                # Get sensor data with error handling
                sensor_data = safe_sensor_get(s.id)
                
                if sensor_data is None:
                    print(f"⚠️ Could not fetch details for sensor {s.id}")
                    continue
                
                # Print basic info (modify as needed)
                print(f"🏙️ Locality: {loc.locality or 'N/A'}")
                print(f"📌 Coordinates: {loc.coordinates.latitude}, {loc.coordinates.longitude}")
                print(f"📊 Latest PM2.5: {sensor_data.latest['value'] if hasattr(sensor_data, 'latest') else 'N/A'} µg/m³")
                print(f"⏰ Last updated: {sensor_data.latest['datetime']['local'] if hasattr(sensor_data, 'latest') else 'N/A'}")

finally:
    client.close()
    print("\n✅ Data collection completed")
