{{config (materialized='table')}}

SELECT
    trips.pickup_hour,
    weather.weather_description,

    count(*) AS trips_count,
    avg(trips.trip_duration_min) as avg_trip_duration_min,
    avg(trips.tip_amount) as avg_tip_amount

FROM {{ ref('source_dim_weather') }} AS weather
LEFT JOIN {{ ref('source_fact_taxi_trips') }} AS trips ON weather.date_measure = trips.pickup_date
AND weather.measure_hour = trips.pickup_hour
WHERE weather_description IS NOT NULL
GROUP BY 1, 2
