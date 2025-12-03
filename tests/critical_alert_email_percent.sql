-- Configuration to set the test severity
{{
  config(
    severity = 'error' 
  )
}}

-- This test fails if the failure count in the model is 5% or more 
-- of the total count in the source model.

WITH total_customers AS (
    -- Get the total population count (denominator)
    SELECT COUNT(*) AS total_count FROM {{ ref('stg_logistic__customers') }}
),

failure_rows AS (
    -- Get the failure count (numerator)
    SELECT COUNT(*) AS failure_count FROM {{ ref('d1_2_email_pattern_failures') }}
)

SELECT
    fr.failure_count,
    tr.total_count
FROM failure_rows fr
CROSS JOIN total_customers tr

-- FAILURE CONDITION: If (Failure Count / Total Count) >= 0.05 (5%)
WHERE (fr.failure_count * 1.0 / tr.total_count) >= 0.05