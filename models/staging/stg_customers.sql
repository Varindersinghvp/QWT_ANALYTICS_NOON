{{config(materialized='table', alias='stg_customers' )}}

select * from {{source('raw_qwt','raw_customers')}}