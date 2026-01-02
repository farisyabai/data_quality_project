{{ config(
    materialized = 'view',
    tags = ['latest_load']
) }}

SELECT *
FROM {{ ref('ss_orders') }}
WHERE dbt_valid_to IS NULL