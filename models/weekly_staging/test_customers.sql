{{ config(
    materialized = 'view',
    tags = ['test_weeks_only']
) }}

SELECT *
FROM {{ ref('customers') }}
WHERE dbt_valid_from = (SELECT MAX(dbt_valid_from) FROM {{ ref('customers') }})