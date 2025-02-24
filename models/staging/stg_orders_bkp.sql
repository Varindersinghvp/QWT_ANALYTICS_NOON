{{config(materialized='incremental', unique_key='OrderID' )}}

select * from {{source('raw_qwt','raw_orders')}}
   
   {% if is_incremental() %}

  -- new
  -- this filter will only be applied on an incremental run
  where OrderDate > (select max(OrderDate) from {{ this }})

{% endif %}
 
