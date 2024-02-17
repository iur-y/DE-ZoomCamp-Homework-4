{{ config(materialized='view') }}

SELECT 
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID,
    DOlocationID

FROM {{ source('raw_data','raw_fhv') }}

WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019

{%- if var("is_test_run", "true") %}
LIMIT 1000
{% endif %}