{{ config(
    materialized = 'incremental',
    unique_key = 'audit_id'
) }}

{%- set audit_schema = '%_weekly_generic' -%} 
{%- set today_suffix = modules.datetime.date.today().strftime('%Y_%m_%d') -%}

{%- set audit_tables = dbt_utils.get_relations_by_pattern(
    schema_pattern=audit_schema,
    table_pattern='%' ~ today_suffix ~ '%' 
) -%}

{# 
   Pre-check columns from the first available table so Jinja knows what exists.
   We store them in a list for the COALESCE logic below.
#}
{%- set available_columns = [] -%}
{%- if audit_tables|length > 0 -%}
    {%- set cols = adapter.get_columns_in_relation(audit_tables[0]) -%}
    {%- for col in cols -%}
        {%- do available_columns.append(col.name.lower()) -%}
    {%- endfor -%}
{%- endif -%}

WITH raw_unioned_failures AS (
    {% if audit_tables|length > 0 %}
        {{ dbt_utils.union_relations(relations=audit_tables) }}
    {% else %}
        SELECT  
            CAST(NULL AS TEXT) as customer_id,
            CAST(NULL AS TEXT) as order_id,
            CAST(NULL AS TEXT) as shipment_id,
            CAST(NULL AS TEXT) as update_id,
            CAST(NULL AS TEXT) as _dbt_source_relation
        LIMIT 0
    {% endif %}
),

standardized_failures AS (
    SELECT
        _dbt_source_relation as source_relation,
        
        -- 1. CLEAN TEST NAME
        REGEXP_REPLACE(
            REPLACE(SPLIT_PART(_dbt_source_relation, '.', 3), '"', ''), 
            '_[0-9]{4}_[0-9]{2}_[0-9]{2}$', 
            ''
        ) as test_name,
        
        -- 2. DYNAMIC PK: Now uses the pre-checked available_columns list
        COALESCE(
            {% if 'customer_id' in available_columns %} CAST(customer_id AS TEXT) {% else %} NULL {% endif %},
            {% if 'order_id' in available_columns %} CAST(order_id AS TEXT) {% else %} NULL {% endif %},
            {% if 'shipment_id' in available_columns %} CAST(shipment_id AS TEXT) {% else %} NULL {% endif %},
            {% if 'update_id' in available_columns %} CAST(update_id AS TEXT) {% else %} NULL {% endif %},
            'AGGREGATE_METRIC'
        ) as primary_key_value,

        -- 3. JSON PAYLOAD
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
  WHERE recorded_at > (SELECT COALESCE(MAX(recorded_at), '1900-01-01'::timestamp) FROM {{ this }})
{% endif %}