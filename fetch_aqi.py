import pandas as pd
from openaq import OpenAQ
import os
from datetime import datetime

def initialize_client(api_key):
    """Initialize and return OpenAQ client"""
    return OpenAQ(api_key=api_key)

def get_locations_data(client, bbox_coords, limit=1000):
    """Fetch location data from OpenAQ API"""
    try:
        locations = client.locations.list(
            bbox=bbox_coords, 
            limit=limit
        )
        return locations.results
    except Exception as e:
        print(f"Error fetching locations: {e}")
        return []

def create_base_dataframe(locations_data, current_time):
    """Create structured DataFrame from locations data"""
    rows = []
    for location in locations_data:
        for sensor in location.sensors:
            rows.append({
                'location_id': location.id,
                'location_name': location.name,
                'location_country': location.country.name,
                'owner': location.owner.name,
                'sensor_id': sensor.id,
                'sensor_name': sensor.name,
                'parameter_name': sensor.parameter.name,
                'parameter_units': sensor.parameter.units,
                'parameter_display_name': sensor.parameter.display_name,
                'value': None,
                'latitude': location.coordinates.latitude,
                'longitude': location.coordinates.longitude,
                'last_updated': None,
                'sysDate': current_time
            })
    return pd.DataFrame(rows)

def fetch_and_update_measurements(client, df):
    """Fetch latest measurements and update DataFrame values"""
    for location_id in df['location_id'].unique():
        try:
            measurements = client.locations.latest(locations_id=location_id)
            
            if measurements.meta.found > 0:
                for latest in measurements.results:
                    mask = ((df['location_id'] == location_id) & 
                           (df['sensor_id'] == latest.sensors_id))
                    if mask.any():
                        df.loc[mask, 'value'] = latest.value
                        df.loc[mask, 'last_updated'] = latest.datetime['utc']
        except Exception as e:
            print(f"Error fetching measurements for location {location_id}: {e}")
            continue
    
    return df[df['value'].notna()]

def save_to_csv(df, file_path):
    """Save DataFrame to CSV (append if file exists)"""
    if os.path.exists(file_path):
        df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8-sig')
        print(f"Data appended to {file_path}")
    else:
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"New file created at {file_path}")

def main():
    """Main function to execute the data pipeline"""
    current_time = datetime.now()
    
    # Initialize client
    client = initialize_client('9532f24560378a32472d71772950f5dd063cfdacbb769e00ea9d4ed05a93fa9a')
    
    # Get locations data
    locations_data = get_locations_data(client, (77.102, 28.404, 77.348, 28.883))
    
    if not locations_data:
        print("No location data retrieved. Exiting.")
        return
    
    # Create base DataFrame
    df = create_base_dataframe(locations_data, current_time)
    
    # Fetch and update measurements
    df = fetch_and_update_measurements(client, df)
    
    # Save to CSV
    save_to_csv(df, 'data/delhi_air_quality_data.csv')
    
    client.close()
    print(f"Pipeline completed successfully. {len(df)} records processed.")

if __name__ == "__main__":
    main()
