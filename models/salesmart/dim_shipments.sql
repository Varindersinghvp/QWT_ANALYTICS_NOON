{{config(materialized='view', schema='salesmart_dev' )}}

select 
ORDERID,
LINENO,
COMPANYNAME,
SHIPMENTDATE,
currentstatus

from {{ ref("trf_shipments")}}
