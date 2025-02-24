{{config(materialized='table',transient= false )}}

select * from {{source('raw_qwt','RAW_EMPLOYEES')}} order by 1