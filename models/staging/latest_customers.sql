{{ config(
    materialized = 'view',
    tags = ['latest_load']
) }}

SELECT *
FROM {{ ref('ss_customers') }}
WHERE dbt_valid_to IS NULL