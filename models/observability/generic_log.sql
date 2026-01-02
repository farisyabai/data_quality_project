{{ config(
    materialized = 'incremental',
    unique_key = 'audit_id'
) }}

{%- set audit_schema = 'dev_weekly_generic' -%} 

{%- set audit_tables = dbt_utils.get_relations_by_pattern(
    schema_pattern=audit_schema,
    table_pattern='%_2025_%' 
) -%}

WITH raw_unioned_failures AS (
    {% if audit_tables|length > 0 %}
        {{ dbt_utils.union_relations(relations=audit_tables) }}
    {% else %}
        SELECT  
            NULL as customer_id,
            NULL as order_id,
            NULL as shipment_id,
            NULL as update_id,
            NULL as _dbt_source_relation
        LIMIT 0
    {% endif %}
),

standardized_failures AS (
    SELECT
        _dbt_source_relation as source_relation,
        
        -- 1. CLEAN TEST NAME: Remove database quotes and the date suffix (_YYYY_MM_DD)
        -- This allows you to group results in your dashboard by test type.
        REGEXP_REPLACE(
            REPLACE(SPLIT_PART(_dbt_source_relation, '.', 3), '"', ''), 
            '_[0-9]{4}_[0-9]{2}_[0-9]{2}$', 
            ''
        ) as test_name,
        
        -- 2. DYNAMIC PK: Coalesce common ID columns
        COALESCE(
            {% if 'customer_id' in raw_unioned_failures %} CAST(customer_id AS TEXT) {% else %} NULL {% endif %},
            {% if 'order_id' in raw_unioned_failures %} CAST(order_id AS TEXT) {% else %} NULL {% endif %},
            {% if 'shipment_id' in raw_unioned_failures %} CAST(shipment_id AS TEXT) {% else %} NULL {% endif %},
            {% if 'update_id' in raw_unioned_failures %} CAST(order_id AS TEXT) {% else %} NULL {% endif %},
            'AGGREGATE_METRIC' -- Changed from N/A to distinguish aggregate tests
        ) as primary_key_value,

        -- 3. JSON PAYLOAD: Captures the specific metrics (count, pct, etc)
        row_to_json(raw_unioned_failures.*)::TEXT as failure_details,
        
        CURRENT_TIMESTAMP as recorded_at
    FROM raw_unioned_failures
)

SELECT
    -- Stable ID for incremental logic
    {{ dbt_utils.generate_surrogate_key(['source_relation', 'primary_key_value', 'recorded_at']) }} as audit_id,
    *
FROM standardized_failures

{% if is_incremental() %}
  WHERE recorded_at > (SELECT MAX(recorded_at) FROM {{ this }})
{% endif %}