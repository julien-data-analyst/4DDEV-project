-- Active: 1776860365260@@127.0.0.1@5434@data_warehouse
##############################################
# Création des tables du Data Warehouse
##############################################

DROP TABLE IF EXISTS public.fact_taxi_trips;
DROP TABLE IF EXISTS public.dim_weather;

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
    prct_pourboire DOUBLE PRECISION
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
    fictif BOOLEAN
);

##############################################
# La création d'index sur les colonnes qu'on va utiliser le plus souvent
##############################################

# Sur les ids et catégorie
CREATE INDEX idx_pickup_hour ON public.fact_taxi_trips (pickup_hour);

CREATE INDEX idx_pickup_dow ON public.fact_taxi_trips (pickup_dow);

CREATE INDEX idx_tranche_distance ON public.fact_taxi_trips (tranche_trip_distance_km);

CREATE INDEX idx_payment_type ON public.fact_taxi_trips (payment_type_str);

CREATE INDEX idx_puzone ON public.fact_taxi_trips (puzone);

CREATE INDEX idx_dozone ON public.fact_taxi_trips (dozone);


# Regarder les index créés
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public';