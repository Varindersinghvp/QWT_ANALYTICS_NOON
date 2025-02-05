{{config(materialized='table', alias='stg_customers', database='QWT_ANALYTICS_UAT' )}}

select * from {{source('raw_qwt','raw_customers')}}