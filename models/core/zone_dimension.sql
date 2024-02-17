{{ config(materialized='table') }}

SELECT *
FROM {{ ref('taxi_zones') }}
WHERE Borough != 'Unknown'