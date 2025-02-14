{{config(materialized='table' , schema='transforming_dev'  )}}

select 
--c.ORDERID,c.LINENO,c.SHIPPERID,c.PRODUCTID,c.EMPLOYEEID,c.SHIPMENTDATE,c.STATUS,
--c.SCD_ID, c.UPDATED_AT,c.VALID_FROM,c.VALID_TO,
c.ORDERID,c.LINENO,d.COMPANYNAME,c.SHIPMENTDATE,c.STATUS as current_status
--,c.DBT_VALID_FROM,c.DBT_VALID_TO
from 
{{ ref("shipments_snapshot")}} c
inner join  {{ ref("lkp_shippers")}} d on c.SHIPPERID=d.ShipperID
where c.DBT_VALID_TO is null