{% test failure_row_rates(model, to, failure_rate) %}

-- This test fails if the row count of 'model' is greater than 
-- 'at_least_percent' of the row count of the 'to' model.

WITH numerator AS (
    -- The failure count (model the test is attached to)
    SELECT COUNT(*) AS failure_count FROM {{ model }}
),

denominator AS (
    -- The total population count (model specified by 'to')
    -- We use ref(to) because 'to' will be passed as a string model name in the YAML.
    SELECT COUNT(*) AS total_count FROM {{ ref(to) }}
)

SELECT
    -- Select the counts to return rows only if the condition is met
    num.failure_count,
    den.total_count,
    ROUND(100.0 * num.failure_count) / den.total_count AS percentage
FROM numerator num
CROSS JOIN denominator den

-- FAILURE CONDITION: If (Failure Count / Total Count) >= percentage
WHERE (num.failure_count * 1.0 / den.total_count) >= {{ failure_rate }}

{% endtest %}