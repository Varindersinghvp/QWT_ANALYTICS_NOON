{{config(materialized='table' , schema=env_var('DBT_TRANSFORMSCHEMA','transforming_dev') )}}

select 
o.orderid,
od.LINENO,
o.CustomerID,
o.employeeid,
o.shipperid,
od.productid,
o.freight,
od.quantity,
od.unitprice,
od.discount,
o.orderdate,
to_decimal((od.UnitPrice * od.Quantity) * (1-od.Discount),9,2) as  LineSalesAmount,
to_decimal((p.UnitCost * od.Quantity),9,2) as CostOfGoodsSold,
to_decimal(((od.UnitPrice * od.Quantity) * (1-od.Discount)) - (p.UnitCost * od.Quantity),9,2) as Margin
  from {{ref("stg_orders")}} as o
  inner join {{ref("stg_orderdetails")}} as od on  o.orderid=od.orderid
  inner join {{ref("stg_products")}} as p on p.productid=od.productid


  