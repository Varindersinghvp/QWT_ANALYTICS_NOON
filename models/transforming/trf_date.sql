{{config(materialized='table', schema='transforming_dev' )}}

{{ dbt_date.get_date_dimension(get_order_mindate(), get_order_maxdate()) }}