{{config(materialized='table', alias='stg_suppliers' )}}

select * from {{source('raw_qwt','RAW_SUPPLIERS')}}