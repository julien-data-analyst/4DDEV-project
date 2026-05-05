{{ config(
    materialized='table',
    schema='marts'
) }}

SELECT
    passenger_count,
    COUNT(id_taxi)        AS nb_trajets,
    SUM(fare_amount)     AS total_depense,
    AVG(prct_pourboire)   AS moy_pourboire
FROM {{ source('public', 'fact_taxi_trips') }}
GROUP BY passenger_count


