SELECT *
FROM {{ ref('stg_logistic__customers') }}
WHERE email !~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]'