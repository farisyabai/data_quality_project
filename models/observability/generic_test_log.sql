{{ config(
    materialized = 'incremental',
    unique_key = 'incident_id'
) }}

{%- set env_prefix = 'prod' if target.name == 'prod' else 'dev' -%}
{%- set audit_schema = env_prefix ~ '_weekly_generic' -%}

{%- set audit_tables = dbt_utils.get_relations_by_pattern(
    schema_pattern=audit_schema,
    table_pattern='%'
) -%}

WITH raw_unioned_failures AS (
    {% if audit_tables | length > 0 %}
        {{ dbt_utils.union_relations(relations=audit_tables) }}
    {% else %}
        -- Fallback if no failure tables exist yet
        SELECT 
            NULL::TEXT as _dbt_source_relation,
            NULL::TEXT as customer_id, 
            NULL::TEXT as order_id, 
            NULL::TEXT as shipment_id,
            NULL::TEXT as update_id
        LIMIT 0
    {% endif %}
),

standardized_failures AS (
    SELECT        
        -- The name of the test that failed
        _dbt_source_relation as source_test_table,
        
        -- Extract just the test name from the full relation string
        split_part(_dbt_source_relation, '.', 3) as test_name,

        -- Capture the actual data that failed as a JSON object
        -- We exclude 'recorded_at' from here if we want the hash to be stable
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
    WHERE incident_id NOT IN (SELECT incident_id FROM {{ this }})
{% endif %}