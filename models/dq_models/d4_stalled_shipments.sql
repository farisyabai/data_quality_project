SELECT
    shipment_id,
    expected_delivery_date,
    actual_delivery_date,
    'D-4: Stalled Shipments' AS rule_violated,
    CURRENT_TIMESTAMP AS incident_timestamp
FROM {{ ref("stg_logistic__shipments") }}
WHERE actual_delivery_date > expected_delivery_date
    AND current_status = 'Delivered'