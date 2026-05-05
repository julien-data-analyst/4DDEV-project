{{ config(materialized='table') }}

SELECT
    passenger_count,
    COUNT(id_taxi)        AS nb_trajets,
    SUM(total_amount)     AS total_depense,
    AVG(prct_pourboire)   AS moy_pourboire
FROM {{ ref('source_fact_taxi_trips') }}
GROUP BY passenger_count
HAVING COUNT(id_taxi) > 10
   AND SUM(total_amount) > 300
   AND AVG(prct_pourboire) > 0.15

