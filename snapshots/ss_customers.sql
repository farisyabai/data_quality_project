{% snapshot ss_customers %}
{{
    config(
      target_schema='snapshots',       
      unique_key='customer_id',            
      strategy='check',                    
      check_cols=['city', 'email', 'phone_number', 'create_at'], 
      invalidate_hard_deletes=True         
    )
}}
SELECT * FROM {{ source('raw','customers') }}
{% endsnapshot %}