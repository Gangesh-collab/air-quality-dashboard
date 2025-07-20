import pandas as pd
import requests
from datetime import datetime
import os
import time

def get_openaq_data():
    url = "https://api.openaq.org/v3/measurements"
    headers = {
        "X-API-Key": ""  # No longer required for v3
    }
    params = {
        'limit': 100,
        'parameter': ['pm25'],
        'sort': 'desc',
        'date_from': datetime.utcnow().strftime('%Y-%m-%dT00:00:00Z')
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Debug: Uncomment to see full response
        # print("API Response:", data)
        
        if 'results' not in data:
            raise ValueError(f"Unexpected API format. Full response: {data}")
            
        df = pd.json_normalize(data['results'])
        
        # Standardize column names
        df = df.rename(columns={
            'value': 'pm25',
            'location': 'location_name',
            'coordinates.latitude': 'latitude',
            'coordinates.longitude': 'longitude'
        })
        
        df['timestamp'] = datetime.utcnow().isoformat()
        return df
        
    except Exception as e:
        print(f"API Error: {str(e)}")
        return pd.DataFrame()

# Main execution
os.makedirs('data', exist_ok=True)

df = get_openaq_data()
if not df.empty:
    df.to_csv('data/latest_aqi.csv', index=False)
    print(f"Saved {len(df)} records")
else:
    # Create empty file to maintain workflow
    pd.DataFrame(columns=['pm25','location_name','latitude','longitude','timestamp']).to_csv('data/latest_aqi.csv', index=False)
    print("Created empty file as fallback")
