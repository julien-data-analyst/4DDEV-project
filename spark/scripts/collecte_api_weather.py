from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import boto3
import requests
from botocore.client import Config

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")
BUCKET_WEATHER = "raw-weather"

_raw_locations = os.getenv("WEATHER_LOCATIONS", "40.7128,-74.0060")
LOCATIONS = [
    (float(lat), float(lon))
    for lat, lon in (loc.split(",") for loc in _raw_locations.split("|"))
]

REQUEST_TIMEOUT = 10

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# MINIO CLIENT
# ─────────────────────────────────────────────────────────────────────────────

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ─────────────────────────────────────────────────────────────────────────────
# API WEATHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_weather(lat: float, lon: float) -> Optional[dict]:
    if not OPENWEATHER_API_KEY:
        raise EnvironmentError("OPENWEATHER_API_KEY manquant")

    params = {
        "lat": lat,
        "lon": lon,
        "appid": OPENWEATHER_API_KEY,
        "units": "metric",
        "lang": "fr",
    }

    try:
        r = requests.get(OPENWEATHER_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        raw = r.json()
    except Exception as e:
        log.error("API error: %s", e)
        return None

    return {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "city_id": raw.get("id"),
        "city_name": raw.get("name"),
        "lat": lat,
        "lon": lon,
        "timestamp_unix": raw.get("dt"),
        "weather_main": raw.get("weather", [{}])[0].get("main"),
        "weather_description": raw.get("weather", [{}])[0].get("description"),
        "temp_celsius": raw.get("main", {}).get("temp"),
        "humidity_pct": raw.get("main", {}).get("humidity"),
        "wind_speed_ms": raw.get("wind", {}).get("speed"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# MINIO WRITE
# ─────────────────────────────────────────────────────────────────────────────

def build_s3_key(event: dict) -> str:
    ts = event.get("ingested_at", datetime.utcnow().isoformat())
    dt = datetime.fromisoformat(ts.replace("Z", ""))
    city = event.get("city_id", "unknown")

    return (
        f"weather/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
        f"{city}_{event.get('timestamp_unix')}.json"
    )


def store_minio(s3, event: dict):
    key = build_s3_key(event)

    s3.put_object(
        Bucket=BUCKET_WEATHER,
        Key=key,
        Body=json.dumps(event).encode("utf-8"),
        ContentType="application/json",
    )

    log.info("Stored → %s", key)


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def run_weather_to_minio():
    s3 = get_s3_client()
    count = 0

    for lat, lon in LOCATIONS:
        event = fetch_weather(lat, lon)
        if event:
            store_minio(s3, event)
            count += 1

    log.info("Done → %d events stored in MinIO", count)
    return {"stored": count}


# ─────────────────────────────────────────────────────────────────────────────
# LOCAL TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run_weather_to_minio())