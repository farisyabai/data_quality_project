{{ config(
    materialized = 'view',
    tags = ['latest_load']
) }}

SELECT *
FROM {{ ref('ss_delivery_updates') }}
WHERE dbt_valid_to IS NULL