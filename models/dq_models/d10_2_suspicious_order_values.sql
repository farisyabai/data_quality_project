SELECT 
    order_id,
    order_value,
    'D-10.2: Suspicious Order Values' AS rule_violated,
    CURRENT_TIMESTAMP AS incident_timestamp
FROM {{ ref('stg_logistic__orders') }}
WHERE order_value <= 0