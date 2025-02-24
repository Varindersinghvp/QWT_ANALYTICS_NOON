{{config(materialized='table' , schema=env_var('DBT_TRANSFORMSCHEMA','transforming_dev') )}}

with cte as (
select a.EMPID,a.LASTNAME,a.FIRSTNAME, a.title, a.hiredate,--a.office,
a.extension,
case when a.reportsto is null then a.empid else a.reportsto end as reportsto,
a.yearsalary, b.address,b.city, b.phone, b.country
from {{ ref("stg_employees")}} a
left join {{ ref("stg_offices")}} b on a.office=b.officeid)
select a.EMPID,a.LASTNAME,a.FIRSTNAME, a.title, a.hiredate,--a.office,
a.extension,--a.reportsto,--b.LASTNAME, b.FIRSTNAME,
concat(b.LASTNAME,' ', b.FIRSTNAME) as Managername,b.title as ManagerTitle, 
a.yearsalary, a.address,a.city, a.phone ,a.country from cte a
left join {{ ref("stg_employees")}} b on a.reportsto=b.empid



  