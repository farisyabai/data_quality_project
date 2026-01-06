{{ config(
    materialized = 'incremental',
    unique_key = 'incident_id'
) }}


{%- set audit_schema = target.schema ~ '_weekly_generic' -%}

{# 
   Log for debugging: This will show up in dbt logs 
   to verify the model is looking in the right place.
#}
{% do log("Searching for test audit tables in schema: " ~ audit_schema, info=True) %}

{%- set audit_tables = dbt_utils.get_relations_by_pattern(
    schema_pattern=audit_schema,
    table_pattern='%'
) -%}

WITH raw_unioned_failures AS (
    {% if audit_tables | length > 0 %}
        {{ dbt_utils.union_relations(relations=audit_tables) }}
    {% else %}
        -- Fallback: If no tables found, return empty set
        SELECT 
            NULL::TEXT as _dbt_source_relation,
            NULL::TEXT as customer_id, 
            NULL::TEXT as order_id, 
            NULL::TEXT as shipment_id,
            NULL::TEXT as update_id
        WHERE 1=0
    {% endif %}
),

standardized_failures AS (
    SELECT        
        _dbt_source_relation as source_test_table,
        
        -- Extract test name from the end of the relation string
        split_part(_dbt_source_relation, '.', 3) as test_name,

        -- Capture failing row data as JSON
        row_to_json(raw_unioned_failures.*)::TEXT as incident_data,
        
        CURRENT_TIMESTAMP as recorded_at
    FROM raw_unioned_failures
    WHERE _dbt_source_relation IS NOT NULL
),

final_processing AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['source_test_table', 'incident_data']) }} as incident_id,
        * FROM standardized_failures
)

SELECT *
FROM final_processing

{% if is_incremental() %}
    -- Only insert records that don't already exist in the log
    WHERE incident_id NOT IN (SELECT incident_id FROM {{ this }})
{% endif %}