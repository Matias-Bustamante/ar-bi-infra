-- back compat for old kwarg name
  
  
        
            
            
            
            
        
    

    

    merge into `data-engineer-509817`.`retail`.`bronze_orders` as DBT_INTERNAL_DEST
        using (
        select
        * from `data-engineer-509817`.`retail`.`bronze_orders__dbt_tmp173254291157`
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id))

    
    when matched then update set
        `order_id` = DBT_INTERNAL_SOURCE.`order_id`,`order_date` = DBT_INTERNAL_SOURCE.`order_date`,`order_customer_id` = DBT_INTERNAL_SOURCE.`order_customer_id`,`order_status` = DBT_INTERNAL_SOURCE.`order_status`
    

    when not matched then insert
        (`order_id`, `order_date`, `order_customer_id`, `order_status`)
    values
        (`order_id`, `order_date`, `order_customer_id`, `order_status`)


    