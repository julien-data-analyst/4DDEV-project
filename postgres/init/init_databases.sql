-- Script exécuté automatiquement par postgres au premier démarrage
-- Crée la base Airflow en plus de la base analytics principale
SELECT 'CREATE DATABASE airflow OWNER ' || current_user
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec

-- Schémas pour le DWH analytics
\c data_warehouse

CREATE TABLE public.fact_taxi_trips (
    id_taxi SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    pickup_date DATE,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance_km DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    pickup_dow INT,
    pickup_hour INT,
    PUBorough TEXT,
    PUZone TEXT,
    PU_service_zone TEXT,
    DOBorough TEXT,
    DOZone TEXT,
    DO_service_zone TEXT,
    total_amount DOUBLE PRECISION,
    tranche_trip_distance_km TEXT,
    payment_type_str TEXT,
    trip_duration_min DOUBLE PRECISION,
    prct_pourboire DOUBLE PRECISION,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE public.dim_weather (
    id_weather SERIAL PRIMARY KEY,
    humidity_pct DOUBLE PRECISION,
    temp_celsius DOUBLE PRECISION,
    datetime_measure TIMESTAMP,
    date_measure DATE,
    weather_description TEXT,
    weather_main TEXT,
    wind_speed_ms DOUBLE PRECISION,    
    measure_dow INT,
    measure_hour INT,
    fictif BOOLEAN,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE SCHEMA IF NOT EXISTS marts;

COMMENT ON SCHEMA marts   IS 'Modèles finaux exposés aux utilisateurs analytiques';