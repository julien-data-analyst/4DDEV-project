{{ config(
    materialized='table',
    schema='marts'
) }}

SELECT
    trips.pickup_hour,
    weather.weather_description,
    count(*) AS trips_count,
    avg(trips.trip_duration_min) as avg_trip_duration_min,
    avg(trips.tip_amount) as avg_tip_amount

FROM {{ source('public', 'fact_taxi_trips') }} AS trips 
INNER JOIN {{ source('public', 'dim_weather') }} AS weather ON weather.date_measure = trips.pickup_date
GROUP BY trips.pickup_hour, weather.weather_description
