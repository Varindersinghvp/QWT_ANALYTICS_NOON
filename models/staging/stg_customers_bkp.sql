{{config(materialized='table', alias='stg_customers_old' )}}

select * from {{source('raw_qwt','raw_customers')}}