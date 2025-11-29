{% snapshot snapshot_shipments %}
{{
    config(
        target_schema='snapshots',
        strategy='timestamp',
        unique_key='shipment_id',
        updated_at='create_at'
    )
}}
SELECT * FROM {{ source('raw_logistic','shipments') }}
{% endsnapshot %}