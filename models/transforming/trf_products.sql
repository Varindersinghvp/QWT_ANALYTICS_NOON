{{config(materialized='table', schema=env_var('DBT_TRANSFORMSCHEMA','transforming_dev')  )}}

select 
p.productid, p.productname,c.categoryname,s.CompanyName as suppliercompany,
s.ContactName as suppliercontact, s.City as suppliercity, s.Country as suppliercountry,
p.quantityperunit, p.unitcost, p.unitprice, p.unitsinstock, p.unitsonorder,
p.unitprice - p.unitcost as profit,
IFF(p.unitsinstock>p.unitsonorder, 'Available','Not Available') as productavailability
from  {{ref("stg_products")}} as p
inner join {{ref("trf_suppliers")}} as s on p.supplierid=s.SUPPLIERID
inner join {{ref("lkp_categories")}} as c on c.CategoryID=p.CategoryID



