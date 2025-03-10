{{config(materialized='view', schema=env_var('DBT_SALESMARTSCHEMA','SALESMART_DEV') )}}

select 
employee_id,
employee_name,
employee_title,
manager_name,
manager_title,
office_city as city,
office_country as country
from {{ ref("trf_employees")}}
