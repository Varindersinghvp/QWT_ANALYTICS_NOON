{{config(materialized='incremental', unique_key=['OrderID','LINENO'] )}}

--select distinct a.*,b.OrderDate from {{source('raw_qwt','raw_orders')}} b 
 select distinct a.*,b.OrderDate from {{ ref("stg_orders")}} b
inner join {{source('raw_qwt','raw_orderdetails')}} a on a.orderid=b.orderid

   {% if is_incremental() %}

  -- new
  -- this filter will only be applied on an incremental run
  where OrderDate >= (select max(OrderDate) from {{ this}})

{% endif %}
 
