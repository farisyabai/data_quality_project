{{ config(
    materialized = 'view',
    tags = ['latest_load']
) }}

SELECT *
FROM {{ ref('ss_shipments') }}
WHERE dbt_valid_to IS NULL