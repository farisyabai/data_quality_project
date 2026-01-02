{{ config(
    materialized = 'table',
    tags = ['incident_model']
) }}

SELECT 
    shipment_id,
    current_status,
    actual_delivery_date,
    'D-14: Missing Actual Delivery Date' AS rule_violated,
    CURRENT_TIMESTAMP AS incident_timestamp
FROM {{ ref("test_shipments") }} 
WHERE actual_delivery_date IS NULL AND current_status = 'Delivered'