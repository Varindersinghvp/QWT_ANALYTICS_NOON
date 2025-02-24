
select ORDERID,count(lineno) as lno
from {{ ref("fact_orders")}}
group by 1 
Having lno<1