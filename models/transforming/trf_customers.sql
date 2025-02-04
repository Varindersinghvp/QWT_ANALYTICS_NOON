{{config(materialized='table', schema='transforming_dev' )}}

select 
c.CustomerID,  c.CompanyName ,c.ContactName ,c.City ,c.Country ,d.DivisionName ,c.Address ,c.Fax ,
c.Phone ,c.PostalCode,
IFF(c.StateProvince='','NA',c.StateProvince) as StateProvince
from {{ ref("stg_customers")}} as c
inner join {{ ref("lkp_divisions")}} as d on d.DivisionID=c.DivisionID
