import pandas as pd
from openaq import OpenAQ
import os
from datetime import datetime

current_time = datetime.now()

# Initialize client
client = OpenAQ(api_key='9532f24560378a32472d71772950f5dd063cfdacbb769e00ea9d4ed05a93fa9a')

# Step 1: Get all locations in Delhi bounding box
locations = client.locations.list(
    bbox=(77.102, 28.404, 77.348, 28.883), 
    limit=1000
)

# Step 2: Create empty DataFrame structure
rows = []
for location in locations.results:
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
            'value': None,  # Empty value to be filled later
            'latitude': location.coordinates.latitude,
            'longitude': location.coordinates.longitude,
            'last_updated': None,
            'sysDate': current_time
        })

# Create DataFrame
df = pd.DataFrame(rows)

# Step 3: Fetch values for each location and update DataFrame
for Location_id in df['location_id'].unique():
    try:
        # Get latest measurements for this location
        measurements = client.locations.latest(locations_id=Location_id)
        
        if measurements.meta.found > 0:
            # Update DataFrame with values
            for latest in measurements.results:
                # Find the corresponding row in DataFrame
                mask = (df['location_id'] == Location_id) & (df['sensor_id'] == latest.sensors_id)
                if mask.any():
                    df.loc[mask, 'value'] = latest.value
                    df.loc[mask, 'last_updated'] = latest.datetime['utc']
    except Exception as e:
        # Silently continue on error
        continue

# Step 4: Save to CSV (append if file exists)
file_path = 'data/delhi_air_quality_data.csv'

if os.path.exists(file_path):
    # Append to existing file without writing headers
    df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8-sig')
else:
    # Create new file with headers
    df.to_csv(file_path, index=False, encoding='utf-8-sig')

client.close()
