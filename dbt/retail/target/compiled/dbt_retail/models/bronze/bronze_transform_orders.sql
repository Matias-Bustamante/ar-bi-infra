

Select 
order_id, 
order_date, 
order_customer_id, 
order_status 
FROM `data-engineer-509817`.`retail`.`staging_orders`



where order_id not in (select order_id from `data-engineer-509817`.`retail`.`bronze_orders`)
