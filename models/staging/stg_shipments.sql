{{config(materialized='table',transient= false ) }}

select ORDERID, LINENO, SHIPPERID, CUSTOMERID PRODUCTID, EMPLOYEEID,
--CAST(LEFT(SHIPMENTDATE,10) AS DATE),
cast(split_part(SHIPMENTDATE,' ',1) as DATE) as SHIPMENTDATE,
--LEFT(trim(SHIPMENTDATE),9) as SHIPMENTDATE,
STATUS
from {{source('raw_qwt','RAW_SHIPMENTS')}}