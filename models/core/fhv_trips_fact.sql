{{ config(materialized='table') }}



SELECT
    pickup_datetime,
    dropOff_datetime AS dropoff_datetime,
    PUlocationID,
    zone1.Borough AS pickup_borough,
    zone1.Zone AS pickup_zone,
    zone1.service_zone AS pickup_service_zone,
    DOlocationID,
    zone2.Borough AS dropoff_borough,
    zone2.Zone AS dropoff_zone,
    zone2.service_zone AS dropoff_service_zone
FROM 
    {{ ref('staging_fhv') }} AS staging_fhv
    INNER JOIN
    {{ ref('zone_dimension') }} AS zone1
    ON
    staging_fhv.PUlocationID = zone1.LocationID
    INNER JOIN
    {{ ref('zone_dimension') }} AS zone2
    ON
    staging_fhv.DOlocationID = zone2.LocationID

{%- if var("is_test_run", "true") %}
LIMIT 1000
{% endif %}
