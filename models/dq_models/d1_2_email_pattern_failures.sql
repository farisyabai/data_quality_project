SELECT 
    customer_id,
    email,
    'D-1.2: Email Pattern Failure' AS rule_violated,
    CURRENT_TIMESTAMP AS incident_timestamp
FROM {{ ref('stg_logistic__customers') }}
WHERE email !~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]'