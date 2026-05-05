select
    id_taxi,
    pickup_datetime,
    pickup_hour,
    pickup_date,
    trip_duration_min,
    trip_distance_km,
    fare_amount,
    tip_amount,
    prct_pourboire,
    payment_type_str
from {{ source('public', 'fact_taxi_trips') }} t