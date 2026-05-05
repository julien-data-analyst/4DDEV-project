select
    id_taxi,
    pickup_datetime,
    pickup_hour,
    trip_distance_km,
    fare_amount,
    tip_amount,
    prct_pourboire,
    payment_type_str
from {{ source('public', 'fact_taxi_trips') }} t