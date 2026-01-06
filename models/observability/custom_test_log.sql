{{ config(
    materialized = 'incremental',
    unique_key = 'incident_id'
) }}

WITH raw_incidents AS (

    -- 1. Source: invalid_email_pattern
    SELECT
        'Invalid Email Pattern' as incident_type,
        'customers' as source_model,
        CAST(customer_id AS TEXT) as entity_id,
        row_to_json(e.*)::TEXT as incident_data
    FROM {{ ref('invalid_email_pattern') }} as e

    UNION ALL

    -- 2. Source: invalid_phone_num_length
    SELECT
        'Invalid Phone Number Length' as incident_type,
        'customers' as source_model,
        CAST(customer_id AS TEXT) as entity_id,
        row_to_json(pl.*)::TEXT as incident_data
    FROM {{ ref('invalid_phone_num_length') }} as pl

    UNION ALL

    -- 3. source: invalid_phone_num_pattern
    SELECT
        'Invalid Phone Number Pattern' as incident_type,
        'customers' as source_model,
        CAST(customer_id AS TEXT) as entity_id,
        row_to_json(pp.*)::TEXT as incident_data
    FROM {{ ref('invalid_phone_num_pattern') }} as pp

    UNION ALL

    -- 4. Source: missing_actual_delivery_date
    SELECT
        'Missing Actual Delivery Date' as incident_type,
        'shipments' as source_model,
        CAST(shipment_id AS TEXT) as entity_id,
        row_to_json(mdd.*)::TEXT as incident_data
    FROM {{ ref('missing_actual_delivery_date') }} as mdd    

    UNION ALL

    -- 5. Source: stalled_shipments
    SELECT
        'Stalled Shipment' as incident_type,
        'shipments' as source_model,
        CAST(shipment_id AS TEXT) as entity_id,
        row_to_json(ss.*)::TEXT as incident_data
    FROM {{ ref('stalled_shipments') }} as ss

    UNION ALL

        -- 6. Source: suspicious_orders
    SELECT
        'Suspicious Orders' as incident_type,
        'orders' as source_model,
        CAST(order_id AS TEXT) as entity_id,
        row_to_json(so.*)::TEXT as incident_data
    FROM {{ ref('suspicious_orders') }} as so

    UNION ALL

    -- 7. Source: teleport_shipments
    SELECT
        'Missing Update Record' as incident_type,
        'delivery_updates' as source_model,
        CAST(shipment_id AS TEXT) as entity_id,
        row_to_json(ts.*)::TEXT as incident_data
    FROM {{ ref('teleport_shipments') }} as ts

),

final_staged AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['incident_type', 'entity_id', 'incident_data']) }} as incident_id,
        *,
        CURRENT_TIMESTAMP as detected_at
    FROM raw_incidents
)

SELECT *
FROM final_staged

{% if is_incremental() %}
    WHERE incident_id NOT IN (SELECT incident_id FROM {{ this }})
{% endif %}