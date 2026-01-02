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
        'Medium' as severity,
        -- Use row_to_json to keep all the specific business details
        row_to_json(e.*)::TEXT as incident_data,
        CURRENT_TIMESTAMP as detected_at
    FROM {{ ref('invalid_email_pattern') }} as e

    UNION ALL

    -- 2. Source: invalid_phone_num_length
    SELECT
        'Invalid Phone Number Length' as incident_type,
        'customers' as source_model,
        CAST(customer_id AS TEXT) as entity_id,
        'Medium' as severity,
        -- Use row_to_json to keep all the specific business details
        row_to_json(pl.*)::TEXT as incident_data,
        CURRENT_TIMESTAMP as detected_at
    FROM {{ ref('invalid_phone_num_length') }} as pl

    UNION ALL

    -- 3. source: invalid_phone_num_pattern
    SELECT
        'Invalid Phone Number Pattern' as incident_type,
        'customers' as source_model,
        CAST(customer_id AS TEXT) as entity_id,
        'Medium' as severity,
        -- Use row_to_json to keep all the specific business details
        row_to_json(pp.*)::TEXT as incident_data,
        CURRENT_TIMESTAMP as detected_at
    FROM {{ ref('invalid_phone_num_pattern') }} as pp

    UNION ALL

    -- 4. Source: missing_actual_delivery_date
    SELECT
        'Missing Actual Delivery Date' as incident_type,
        'shipments' as source_model,
        CAST(shipment_id AS TEXT) as entity_id,
        'Medium' as severity,
        -- Use row_to_json to keep all the specific business details
        row_to_json(mdd.*)::TEXT as incident_data,
        CURRENT_TIMESTAMP as detected_at
    FROM {{ ref('missing_actual_delivery_date') }} as mdd    

    UNION ALL

    -- 5. Source: stalled_shipments
    SELECT
        'Stalled Shipment' as incident_type,
        'shipments' as source_model,
        CAST(shipment_id AS TEXT) as entity_id,
        'Medium' as severity,
        -- Use row_to_json to keep all the specific business details
        row_to_json(ss.*)::TEXT as incident_data,
        CURRENT_TIMESTAMP as detected_at
    FROM {{ ref('stalled_shipments') }} as ss

    UNION ALL

        -- 6. Source: suspicious_orders
    SELECT
        'Suspicious Orders' as incident_type,
        'orders' as source_model,
        CAST(order_id AS TEXT) as entity_id,
        'Medium' as severity,
        -- Use row_to_json to keep all the specific business details
        row_to_json(so.*)::TEXT as incident_data,
        CURRENT_TIMESTAMP as detected_at
    FROM {{ ref('suspicious_orders') }} as so

    UNION ALL

    -- 7. Source: teleport_shipments
    SELECT
        'Missing Update Record' as incident_type,
        'delivery_updates' as source_model,
        CAST(shipment_id AS TEXT) as entity_id,
        'Low' as severity,
        row_to_json(ts.*)::TEXT as incident_data,
        CURRENT_TIMESTAMP as detected_at
    FROM {{ ref('teleport_shipments') }} as ts

)

SELECT
    -- Create a unique ID for every incident instance
    {{ dbt_utils.generate_surrogate_key(['incident_type', 'entity_id', 'detected_at']) }} as incident_id,
    *
FROM raw_incidents

{% if is_incremental() %}
    -- Only add new incidents detected since the last run
    WHERE detected_at > (SELECT MAX(detected_at) FROM {{ this }})
{% endif %}