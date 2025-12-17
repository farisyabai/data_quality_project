SELECT 
    customer_id,
    phone_number,
    'D-2.1: Phone Number Pattern Failure' AS rule_violated,
    CURRENT_TIMESTAMP AS incident_timestamp
FROM {{ ref('stg_logistic__customers') }}
WHERE phone_number ~ '^\+628' OR phone_number ~ '^08'