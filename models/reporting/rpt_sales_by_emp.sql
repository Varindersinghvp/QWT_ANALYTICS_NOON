{{config(materialized='view', schema='reporting_dev' )}}

select c.country as office_country,b.COMPANYNAME,b.CONTACTNAME,
count(a.ORDERID) as total_orders, 
sum(a.quantity) as total_quantity,
sum(a.LINESALESAMOUNT) as total_sales,
avg(a.margin) as avg_margin
from {{ref("fact_orders")}} a
inner join {{ref("dim_customers")}} b on a.customerid=b.customerid
inner join  {{ref("dim_employees")}} c on a.employeeid=c.empid
group by 1,2,3