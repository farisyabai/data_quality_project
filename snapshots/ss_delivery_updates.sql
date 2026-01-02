{% snapshot ss_delivery_updates %}
{{
    config(
      target_schema='snapshots',       
      unique_key='update_id',            
      strategy='check',                    
      check_cols=['update_timestamp', 'status'], 
      invalidate_hard_deletes=True         
    )
}}
SELECT * FROM {{ source('raw','delivery_updates') }}
{% endsnapshot %}