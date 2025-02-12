{{config(materialized='view', schema='reporting_dev' )}}

select b.COMPANYNAME as customername,b.CONTACTNAME as contactname,
min(a.orderdate) as first_order_date,
--min(d.day_of_week_name) as first_order_day,
max(a.orderdate) as recent_order_date, 
--max(d.day_of_week_name) as recent_order_Day,
count(a.ORDERID) as total_orders,
sum(p.productid) as noofproducts,
sum(a.quantity) as total_quantity,
sum(a.LINESALESAMOUNT) as total_sales,
avg(a.margin) as avg_margin
from {{ref('fact_orders')}} a
inner join {{ref('dim_customers')}} b on a.customerid=b.customerid
inner join  {{ref('dim_employees')}} c on a.employeeid=c.EMPLOYEE_ID
inner join {{ref('dim_products')}} p on p.productid=a.productid
--inner join QWT_ANALYTICS_DEV.SALESMART_DEV.DIM_DATE d on a.orderdate=d.date_day
group by 1,2 order by total_sales desc 