### Full Script

```python
import requests
import datetime
import pandas as pd
from google.cloud import bigquery
import os

# Set these as environment variables in your Cloud Function
STORMGLASS_API_KEY = os.environ.get("STORMGLASS_API_KEY")
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")

LAT = 54.2361   # Approx Isle of Man
LNG = -4.5481

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

def ensure_table_exists(client):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    try:
        client.get_table(table_ref)
    except Exception:
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("wave_height", "FLOAT"),
            bigquery.SchemaField("wave_direction", "FLOAT"),
            bigquery.SchemaField("wave_period", "FLOAT"),
            bigquery.SchemaField("swell_height", "FLOAT"),
            bigquery.SchemaField("swell_direction", "FLOAT"),
            bigquery.SchemaField("swell_period", "FLOAT"),
            bigquery.SchemaField("secondary_swell", "FLOAT"),
            bigquery.SchemaField("wind_wave_height", "FLOAT"),
            bigquery.SchemaField("wind_wave_direction", "FLOAT"),
            bigquery.SchemaField("wind_wave_period", "FLOAT"),
            bigquery.SchemaField("water_temperature", "FLOAT"),
            bigquery.SchemaField("tide_height", "FLOAT"),
            bigquery.SchemaField("air_temperature", "FLOAT"),
            bigquery.SchemaField("wind_speed", "FLOAT"),
            bigquery.SchemaField("wind_direction", "FLOAT"),
            bigquery.SchemaField("wind_gust", "FLOAT"),
            bigquery.SchemaField("sunrise", "STRING"),
            bigquery.SchemaField("sunset", "STRING"),
            bigquery.SchemaField("first_light", "STRING"),
            bigquery.SchemaField("last_light", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)

def update_bigquery_table(df):
    client = bigquery.Client()
    ensure_table_exists(client)

    start_date = datetime.datetime.utcnow().date()
    end_date = start_date + datetime.timedelta(days=5)

    delete_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE DATE(timestamp) BETWEEN '{start_date}' AND '{end_date}'
    """

    client.query(delete_query).result()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    import io
    json_data = df.to_json(orient="records", lines=True)

    load_job = client.load_table_from_file(
        file_obj=io.StringIO(json_data),
        destination=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        job_config=job_config,
        rewind=True,
    )
    load_job.result()

def main(request=None):
    df = fetch_surf_data()
    update_bigquery_table(df)
    return "Surf forecast data updated successfully!"
```
