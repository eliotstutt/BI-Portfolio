### Full Script (with table creation removed)

```python
import requests
import datetime
import pandas as pd
from google.cloud import bigquery
import os

# These are expected as environment variables in your Cloud Function:
# - STORMGLASS_API_KEY: Your StormGlass API key
# - PROJECT_ID: Your GCP project ID
# - DATASET_ID: The dataset name in BigQuery
# - TABLE_ID: The BigQuery table name

STORMGLASS_API_KEY = os.environ.get("STORMGLASS_API_KEY")
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")

# Coordinates for Gansey, Isle of Man
LAT = 54.0713
LNG = -4.6585

# Convert m/s to mph
def ms_to_mph(ms):
    return ms * 2.23694

def fetch_surf_data():
    start = datetime.datetime.utcnow()
    end = start + datetime.timedelta(days=5)

    url = "https://api.stormglass.io/v2/weather/point"

    params = {
        "lat": LAT,
        "lng": LNG,
        "params": ",".join([
            "waveHeight","waveDirection","wavePeriod","swellHeight","swellDirection","swellPeriod",
            "secondarySwellHeight","windWaveHeight","windWaveDirection","windWavePeriod","waterTemperature",
            "tide","airTemperature","windSpeed","windDirection","gust","sunrise","sunset","firstLight","lastLight"
        ]),
        "start": int(start.timestamp()),
        "end": int(end.timestamp())
    }

    headers = {"Authorization": STORMGLASS_API_KEY}

    response = requests.get(url, params=params, headers=headers)
    data = response.json()

    records = []
    for hour in data.get("hours", []):
        time = hour["time"]
        record = {
            "timestamp": time,
            "wave_height": hour.get("waveHeight", {}).get("noaa", 0),
            "wave_direction": hour.get("waveDirection", {}).get("noaa", 0),
            "wave_period": hour.get("wavePeriod", {}).get("noaa", 0),
            "swell_height": hour.get("swellHeight", {}).get("noaa", 0),
            "swell_direction": hour.get("swellDirection", {}).get("noaa", 0),
            "swell_period": hour.get("swellPeriod", {}).get("noaa", 0),
            "secondary_swell": hour.get("secondarySwellHeight", {}).get("noaa", 0),
            "wind_wave_height": hour.get("windWaveHeight", {}).get("noaa", 0),
            "wind_wave_direction": hour.get("windWaveDirection", {}).get("noaa", 0),
            "wind_wave_period": hour.get("windWavePeriod", {}).get("noaa", 0),
            "water_temperature": hour.get("waterTemperature", {}).get("noaa", 0),
            "tide_height": hour.get("tide", {}).get("sg", 0),
            "air_temperature": hour.get("airTemperature", {}).get("noaa", 0),
            "wind_speed": ms_to_mph(hour.get("windSpeed", {}).get("noaa", 0)),
            "wind_direction": hour.get("windDirection", {}).get("noaa", 0),
            "wind_gust": ms_to_mph(hour.get("gust", {}).get("noaa", 0)),
            "sunrise": hour.get("sunrise", {}).get("noaa", None),
            "sunset": hour.get("sunset", {}).get("noaa", None),
            "first_light": hour.get("firstLight", {}).get("noaa", None),
            "last_light": hour.get("lastLight", {}).get("noaa", None),
        }
        records.append(record)

    return pd.DataFrame(records)
