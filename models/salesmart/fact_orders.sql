{{config(materialized='view', schema='salesmart_dev', post_hook='grant usage on schema QWT_ANALYTICS_DEV.SALESMART_DEV to role sysadmin ; grant select on view QWT_ANALYTICS_DEV.SALESMART_DEV.fact_orders to role sysadmin;' )}}

select 
*
from {{ ref("trf_orders")}}
