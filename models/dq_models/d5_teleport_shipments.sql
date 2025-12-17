WITH status_rank AS (
    SELECT 
        s.shipment_id,
        COUNT(d.update_id) AS update_per_shipment,
        MAX(
            CASE 
                WHEN d.status = 'Created' THEN 1
                WHEN d.status = 'Dispatched' THEN 2
                WHEN d.status = 'In Transit' THEN 3
                WHEN d.status = 'Delivered' THEN 4
                WHEN d.status = 'Delayed' THEN 4.5
                WHEN d.status IS NULL THEN 0
                ELSE 0.5
            END
        ) AS normalized_status,
        CURRENT_TIMESTAMP AS incident_timestamp
    FROM {{ ref("stg_logistic__shipments") }} AS s
    LEFT JOIN {{ ref("stg_logistic__delivery_updates") }} AS d
        ON s.shipment_id = d.shipment_id
    GROUP BY s.shipment_id
)
SELECT *
FROM status_rank
WHERE update_per_shipment < 3
  AND normalized_status = 4