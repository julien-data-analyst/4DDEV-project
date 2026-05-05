{{ config(materialized='table') }}
select
    id_taxi,
    pickup_datetime,
    pickup_date,
    pickup_hour,
    pickup_date,
    trip_duration_min,
    trip_distance_km,
    fare_amount,
    tip_amount,
    total_amount,
    prct_pourboire,
    payment_type_str,
    passenger_count
from {{ source('public', 'fact_taxi_trips') }} t