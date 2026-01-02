{{ config(
    materialized = 'table',
    tags = ['incident_model']
) }}

SELECT 
    customer_id,
    phone_number,
    LENGTH(phone_number) AS phone_number_length,
    'D-2.2: Phone Number Length Failure' AS rule_violated,
    CURRENT_TIMESTAMP AS incident_timestamp
FROM {{ ref('test_customers') }}
WHERE LENGTH(phone_number) NOT BETWEEN 12 AND 14 