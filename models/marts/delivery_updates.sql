{{ config(
    materialized = 'incremental',
    unique_key = 'update_id',
    incremental_strategy = 'merge'
) }}

SELECT *
FROM {{ref('latest_delivery_updates')}}

{% if is_incremental() %}
    
    WHERE dbt_valid_from > (SELECT MAX(dbt_valid_from) FROM {{ this }})

{% endif %}