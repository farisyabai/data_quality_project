SELECT 
    shipment_id,
    current_status,
    actual_delivery_date,
    'D-14: Missing Actual Delivery Date' AS rule_violated,
    CURRENT_TIMESTAMP AS incident_timestamp
FROM {{ ref("stg_logistic__shipments") }} 
WHERE actual_delivery_date IS NULL AND current_status = 'Delivered'