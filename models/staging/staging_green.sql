{{ config(materialized='view') }}

-- ROW_NUMBER() will make rows unique based on the combination of
-- vendor_id and pickup_datetime
-- allowing for these two columns to be used as a surrogate key
WITH unique_vendor_pickupdate_combination AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY CAST(vendor_id AS INTEGER), pickup_datetime) AS row_numbr
FROM {{ source('raw_data','raw_green') }}
WHERE vendor_id IS NOT NULL 
)

SELECT 
    CAST(vendor_id AS INTEGER) AS vendor_id,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    CAST(rate_code AS NUMERIC) AS rate_code,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    airport_fee,
    total_amount,
    CAST(payment_type AS NUMERIC) AS payment_type,
    {{case_payment_type('payment_type')}} AS payment_type_description,
    CAST(trip_type AS NUMERIC) AS trip_type,
    imp_surcharge,
    CAST(pickup_location_id AS NUMERIC) AS pickup_location_id,
    CAST(dropoff_location_id AS NUMERIC) AS dropoff_location_id
FROM unique_vendor_pickupdate_combination
WHERE row_numbr = 1 AND EXTRACT(YEAR FROM pickup_datetime) = 2019

{%- if var("is_test_run", "true") %}
LIMIT 1000
{% endif %}