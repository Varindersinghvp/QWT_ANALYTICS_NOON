{{config(materialized='view', schema=env_var('DBT_SALESMARTSCHEMA','SALESMART_DEV') )}}

select 
*
from {{ ref("trf_employees")}}
