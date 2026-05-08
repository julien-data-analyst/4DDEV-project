{{ config(
    materialized='incremental',
    schema='marts'
) }}
SELECT DISTINCT
    t.id_taxi,
    t.pickup_datetime,
    t.pickup_date,
    t.pickup_hour,
    t.tranche_trip_distance_km,
    t.fare_amount,
    t.tip_amount,
    t.prct_pourboire,
    t.payment_type_str,
    w.weather_description,
    w.weather_main
FROM {{ source('public', 'fact_taxi_trips') }} t
LEFT JOIN {{ source('public', 'dim_weather') }} w
    ON t.pickup_date = w.date_measure AND t.pickup_hour = w.measure_hour
{% if is_incremental() %}
WHERE t.pickup_datetime > (SELECT MAX(pickup_datetime) FROM {{ this }})
{% endif %}