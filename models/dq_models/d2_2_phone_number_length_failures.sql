SELECT 
    customer_id,
    phone_number,
    LENGTH(phone_number) AS phone_number_length,
    'D-2.2: Phone Number Length Failure' AS rule_violated,
    CURRENT_TIMESTAMP AS incident_timestamp
FROM {{ ref('stg_logistic__customers') }}
WHERE phone_number NOT IN (
    SELECT 
        phone_number
    FROM customers
    WHERE 
        (phone_number ~ '^08'   AND LENGTH(phone_number) BETWEEN 10 AND 12)
     OR (phone_number ~ '^\+628' AND LENGTH(phone_number) BETWEEN 12 AND 14)
)