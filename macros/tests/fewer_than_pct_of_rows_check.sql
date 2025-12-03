{% test fewer_than_pct_of_rows_check(model, to, at_least_percent) %}

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
    den.total_count
FROM numerator num
CROSS JOIN denominator den

-- FAILURE CONDITION: If (Failure Count / Total Count) >= percentage
WHERE (num.failure_count * 1.0 / den.total_count) >= {{ at_least_percent }}

{% endtest %}