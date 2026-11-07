{{ config(
    materialized='table',
    alias='orders_summary' 
) }}

SELECT
    order_customer_id, 
    COUNT(order_id) AS total_orders,
    MAX(order_date) AS latest_order_date
FROM {{ source('retail', 'orders') }}
WHERE order_status != 'CANCELED'
GROUP BY order_customer_id