{% snapshot snapshot_shipments %}
{{
    config(
      target_schema='snapshots',       
      unique_key='shipment_id',            
      strategy='check',                    
      check_cols=['current_status', 'actual_delivery_date'], 
      invalidate_hard_deletes=True         
    )
}}
SELECT * FROM {{ source('raw_logistic','shipments') }}
{% endsnapshot %}