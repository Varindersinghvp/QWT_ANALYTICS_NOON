{{config(materialized='table' , schema='transforming_dev')}}


select a.EMPID,a.LASTNAME,a.FIRSTNAME, a.title, a.hiredate,--a.office,
a.extension,
iff (c.firstname is null ,a.firstname, c.firstname) as managername,
iff (c.title is null ,a.title, c.title) as managertitle,
a.yearsalary, b.address,b.city, b.phone
from {{ ref("stg_employees")}} a
left join {{ ref("stg_employees")}} c on a.reportsto=c.empid
left join {{ ref("stg_offices")}} b on a.office=b.officeid



  