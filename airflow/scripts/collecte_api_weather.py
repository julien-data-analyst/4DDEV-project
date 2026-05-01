"""
collecte_api_weather.py
=======================
DAG Airflow – Collecte STREAMING de la météo via OpenWeatherMap.

Architecture :
  OpenWeatherMap API  →  Producer Kafka (topic: weather-stream)
                      →  Consumer Spark Structured Streaming
                      →  Minio (raw-weather / Parquet partitionné)
                      →  PostgreSQL (table raw.weather_events)

Fréquence : toutes les heures (schedule Airflow + boucle interne).
Villes     : configurées via WEATHER_LOCATIONS dans .env
             format "lat,lon|lat,lon|..."

Le fichier expose :
  - fetch_weather()          : appel API pour une localisation
  - produce_to_kafka()       : envoie dans le topic Kafka
  - consume_and_store()      : consomme Kafka → Minio + Postgres
  - DAG Airflow              : orchestre le tout
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import boto3
import psycopg2
import requests
from botocore.client import Config

# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")
BUCKET_WEATHER = "raw-weather"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_WEATHER", "weather-stream")

POSTGRES_CONN_STR = os.getenv(
    "POSTGRES_CONN",
    "postgresql+psycopg2://dwh_user:dwh_password_change_me@postgres:5432/analytics",
)

# Villes NYC (lat, lon) – modifiables via WEATHER_LOCATIONS
_raw_locations = os.getenv("WEATHER_LOCATIONS", "40.7128,-74.0060|40.6413,-73.7781")
LOCATIONS: list[tuple[float, float]] = [
    (float(lat), float(lon))
    for lat, lon in (loc.split(",") for loc in _raw_locations.split("|"))
]

REQUEST_TIMEOUT = 10   # secondes
LANG = "en"

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  Clients
# ─────────────────────────────────────────────────────────────────────────────

def get_s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def get_pg_conn():
    """Retourne une connexion psycopg2 depuis la chaîne SQLAlchemy-style."""
    # Parsing simple de la DSN SQLAlchemy → psycopg2
    import re
    m = re.match(
        r"postgresql\+psycopg2://(?P<user>[^:]+):(?P<pwd>[^@]+)@(?P<host>[^:]+):(?P<port>\d+)/(?P<db>.+)",
        POSTGRES_CONN_STR,
    )
    if not m:
        raise ValueError(f"DSN invalide : {POSTGRES_CONN_STR}")
    return psycopg2.connect(
        host=m["host"], port=int(m["port"]),
        user=m["user"], password=m["pwd"], dbname=m["db"],
    )


# ─────────────────────────────────────────────────────────────────────────────
#  1. Collecte depuis l'API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_weather(lat: float, lon: float) -> Optional[dict]:
    """
    Appelle l'API OpenWeatherMap et retourne un dict normalisé,
    ou None si l'appel échoue.
    """
    if not OPENWEATHER_API_KEY:
        raise EnvironmentError("OPENWEATHER_API_KEY non défini !")

    params = {
        "lat": lat,
        "lon": lon,
        "appid": OPENWEATHER_API_KEY,
        "lang": LANG,
        "units": "metric",   # Celsius
    }

    try:
        resp = requests.get(OPENWEATHER_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        raw = resp.json()
    except requests.RequestException as exc:
        log.error("Erreur API météo (lat=%.4f, lon=%.4f) : %s", lat, lon, exc)
        return None

    # Normalisation du payload
    return {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "city_id": raw.get("id"),
        "city_name": raw.get("name"),
        "country": raw.get("sys", {}).get("country"),
        "lat": lat,
        "lon": lon,
        "timestamp_unix": raw.get("dt"),
        "timestamp_utc": datetime.utcfromtimestamp(raw["dt"]).isoformat() + "Z" if raw.get("dt") else None,
        "weather_main": raw.get("weather", [{}])[0].get("main"),
        "weather_description": raw.get("weather", [{}])[0].get("description"),
        "weather_icon": raw.get("weather", [{}])[0].get("icon"),
        "temp_celsius": raw.get("main", {}).get("temp"),
        "feels_like_celsius": raw.get("main", {}).get("feels_like"),
        "temp_min_celsius": raw.get("main", {}).get("temp_min"),
        "temp_max_celsius": raw.get("main", {}).get("temp_max"),
        "humidity_pct": raw.get("main", {}).get("humidity"),
        "pressure_hpa": raw.get("main", {}).get("pressure"),
        "visibility_m": raw.get("visibility"),
        "wind_speed_ms": raw.get("wind", {}).get("speed"),
        "wind_deg": raw.get("wind", {}).get("deg"),
        "wind_gust_ms": raw.get("wind", {}).get("gust"),
        "clouds_pct": raw.get("clouds", {}).get("all"),
        "rain_1h_mm": raw.get("rain", {}).get("1h"),
        "snow_1h_mm": raw.get("snow", {}).get("1h"),
        "sunrise_unix": raw.get("sys", {}).get("sunrise"),
        "sunset_unix": raw.get("sys", {}).get("sunset"),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  2. Producteur Kafka
# ─────────────────────────────────────────────────────────────────────────────

def produce_to_kafka(events: list[dict]) -> None:
    """
    Envoie chaque événement météo dans le topic Kafka.
    Utilise confluent_kafka en priorité, sinon kafka-python.
    """
    try:
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

        def delivery_report(err, msg):
            if err:
                log.error("Kafka delivery error : %s", err)
            else:
                log.debug("Message livré → partition %d offset %d", msg.partition(), msg.offset())

        for event in events:
            key = f"{event['city_id']}_{event['timestamp_unix']}"
            producer.produce(
                KAFKA_TOPIC,
                key=key.encode(),
                value=json.dumps(event).encode(),
                callback=delivery_report,
            )
        producer.flush(timeout=30)
        log.info("%d événements envoyés dans Kafka (topic: %s)", len(events), KAFKA_TOPIC)

    except ImportError:
        # Fallback kafka-python
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode() if k else None,
        )
        for event in events:
            key = f"{event['city_id']}_{event['timestamp_unix']}"
            producer.send(KAFKA_TOPIC, key=key, value=event)
        producer.flush()
        log.info("%d événements envoyés dans Kafka (topic: %s)", len(events), KAFKA_TOPIC)


# ─────────────────────────────────────────────────────────────────────────────
#  3. Consommateur : Kafka → Minio + Postgres
# ─────────────────────────────────────────────────────────────────────────────

def consume_and_store(max_messages: int = 100, timeout_s: float = 30.0) -> int:
    """
    Lit jusqu'à max_messages messages dans le topic Kafka et les persiste dans :
      - Minio     : raw-weather/YYYY/MM/DD/HH/<city_id>_<ts>.json
      - PostgreSQL: table raw.weather_events

    Retourne le nombre de messages traités.
    """
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "weather-consumer-dwh",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])

    s3 = get_s3_client()
    pg = get_pg_conn()
    _ensure_pg_table(pg)

    processed = 0
    deadline = time.time() + timeout_s

    try:
        while processed < max_messages and time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                log.error("Erreur Kafka consumer : %s", msg.error())
                continue

            try:
                event = json.loads(msg.value().decode())
                _store_minio(s3, event)
                _store_postgres(pg, event)
                processed += 1
            except Exception as exc:
                log.error("Erreur traitement message : %s", exc)

    finally:
        consumer.close()
        pg.close()

    log.info("%d événements météo persistés.", processed)
    return processed


def _s3_key_weather(event: dict) -> str:
    ts = event.get("timestamp_utc", event.get("ingested_at", ""))
    try:
        dt = datetime.fromisoformat(ts.rstrip("Z"))
    except Exception:
        dt = datetime.utcnow()
    city_id = event.get("city_id", "unknown")
    ts_unix = event.get("timestamp_unix", int(dt.timestamp()))
    return (
        f"weather_events/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}"
        f"/{dt.hour:02d}/{city_id}_{ts_unix}.json"
    )


def _store_minio(s3: boto3.client, event: dict) -> None:
    key = _s3_key_weather(event)
    s3.put_object(
        Bucket=BUCKET_WEATHER,
        Key=key,
        Body=json.dumps(event, ensure_ascii=False).encode(),
        ContentType="application/json",
    )
    log.debug("Minio ← %s", key)


def _ensure_pg_table(conn) -> None:
    ddl = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.weather_events (
        id                BIGSERIAL PRIMARY KEY,
        ingested_at       TIMESTAMPTZ NOT NULL,
        city_id           BIGINT,
        city_name         TEXT,
        country           CHAR(2),
        lat               DOUBLE PRECISION,
        lon               DOUBLE PRECISION,
        timestamp_unix    BIGINT,
        timestamp_utc     TIMESTAMPTZ,
        weather_main      TEXT,
        weather_description TEXT,
        temp_celsius      DOUBLE PRECISION,
        feels_like_celsius DOUBLE PRECISION,
        temp_min_celsius  DOUBLE PRECISION,
        temp_max_celsius  DOUBLE PRECISION,
        humidity_pct      SMALLINT,
        pressure_hpa      SMALLINT,
        visibility_m      INT,
        wind_speed_ms     DOUBLE PRECISION,
        wind_deg          SMALLINT,
        wind_gust_ms      DOUBLE PRECISION,
        clouds_pct        SMALLINT,
        rain_1h_mm        DOUBLE PRECISION,
        snow_1h_mm        DOUBLE PRECISION,
        sunrise_unix      BIGINT,
        sunset_unix       BIGINT
    );
    CREATE INDEX IF NOT EXISTS idx_weather_city_ts
        ON raw.weather_events (city_id, timestamp_utc DESC);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def _store_postgres(conn, event: dict) -> None:
    sql = """
    INSERT INTO raw.weather_events (
        ingested_at, city_id, city_name, country, lat, lon,
        timestamp_unix, timestamp_utc, weather_main, weather_description,
        temp_celsius, feels_like_celsius, temp_min_celsius, temp_max_celsius,
        humidity_pct, pressure_hpa, visibility_m,
        wind_speed_ms, wind_deg, wind_gust_ms,
        clouds_pct, rain_1h_mm, snow_1h_mm, sunrise_unix, sunset_unix
    ) VALUES (
        %(ingested_at)s, %(city_id)s, %(city_name)s, %(country)s, %(lat)s, %(lon)s,
        %(timestamp_unix)s, %(timestamp_utc)s, %(weather_main)s, %(weather_description)s,
        %(temp_celsius)s, %(feels_like_celsius)s, %(temp_min_celsius)s, %(temp_max_celsius)s,
        %(humidity_pct)s, %(pressure_hpa)s, %(visibility_m)s,
        %(wind_speed_ms)s, %(wind_deg)s, %(wind_gust_ms)s,
        %(clouds_pct)s, %(rain_1h_mm)s, %(snow_1h_mm)s, %(sunrise_unix)s, %(sunset_unix)s
    )
    ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, event)
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
#  4. Orchestration complète (producer + consumer) pour une exécution horaire
# ─────────────────────────────────────────────────────────────────────────────

def run_weather_pipeline() -> dict:
    """
    Collecte la météo pour toutes les villes configurées, publie dans Kafka,
    puis consomme et persiste. Appelé par le DAG Airflow toutes les heures.
    """
    events = []
    for lat, lon in LOCATIONS:
        data = fetch_weather(lat, lon)
        if data:
            events.append(data)
            log.info("Météo collectée : %s (%.1f°C)", data.get("city_name"), data.get("temp_celsius"))

    if not events:
        log.warning("Aucun événement météo collecté.")
        return {"produced": 0, "consumed": 0}

    produce_to_kafka(events)
    consumed = consume_and_store(max_messages=len(events) * 2, timeout_s=60)

    return {"produced": len(events), "consumed": consumed}


# ─────────────────────────────────────────────────────────────────────────────
#  DAG Airflow
# ─────────────────────────────────────────────────────────────────────────────

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
    import datetime as dt_module

    default_args = {
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": dt_module.timedelta(minutes=5),
        "email_on_failure": False,
    }

    with DAG(
        dag_id="collecte_api_weather_streaming",
        description="Collecte horaire météo OpenWeatherMap → Kafka → Minio + Postgres",
        schedule_interval="0 * * * *",   # toutes les heures pile
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args,
        tags=["ingestion", "streaming", "weather", "kafka", "minio"],
    ) as dag:

        task_weather = PythonOperator(
            task_id="fetch_produce_consume_weather",
            python_callable=run_weather_pipeline,
        )

except ImportError:
    pass


# ─────────────────────────────────────────────────────────────────────────────
#  Exécution standalone
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run_weather_pipeline()
    print("Résultat :", result)
