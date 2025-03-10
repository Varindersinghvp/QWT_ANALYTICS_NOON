{{config(materialized='view', schema=env_var('DBT_SALESMARTSCHEMA','SALESMART_DEV') )}}

select 
ORDERID,
LINENO,
COMPANYNAME,
SHIPMENTDATE,
currentstatus

from {{ ref("trf_shipments")}}
