{{config(materialized='view', schema='reporting_dev' )}}

--{% set v_linenos=[1,2,3,4] %}
{% set v_linenos=get_order_linenos() %}

SELECT ORDERID,

--sum(case when LINENO=1  then LINESALESAMOUNT else 0 end) as LINENO1_sales,
--sum(case when LINENO=2  then LINESALESAMOUNT else 0 end) as LINENO2_sales,
--sum(case when LINENO=3  then LINESALESAMOUNT else 0 end) as LINENO3_sales,

{% for linenos in v_linenos -%}
sum(case when LINENO={{linenos}}  then LINESALESAMOUNT else 0 end) as LINENO{{linenos}}_sales,
{% endfor %}

sum(LINESALESAMOUNT) as Total_sales
from {{ref('fact_orders')}}
--where ORDERID=10402
 group by ORDERID