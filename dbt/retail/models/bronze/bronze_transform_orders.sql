{{
    config(
        materialized='incremental', 
        unique_key='order_id', 
        incremental_strategy="merge",
        on_schema_change='sync_all_columns', 
        alias="bronze_orders"
    )

}}

Select 
order_id, 
order_date, 
order_customer_id, 
order_status 
FROM {{ source("retail", "staging_orders")}}

{% if is_incremental() %}

where order_id not in (select order_id from {{this}})
{% endif %}
