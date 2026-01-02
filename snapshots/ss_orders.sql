{% snapshot ss_orders %}
{{
    config(
      target_schema='snapshots',       
      unique_key='order_id',            
      strategy='check',                    
      check_cols=['order_date', 'order_value'], 
      invalidate_hard_deletes=True         
    )
}}
SELECT * FROM {{ source('raw','orders') }}
{% endsnapshot %}