
-------------- first get_order_linenos

{% macro get_order_linenos() %}

{% set order_linenos_query %}

select distinct
lineno
from {{ ref('fact_orders') }}
order by 1
{% endset %}
{% set results = run_query(order_linenos_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}
--{{ log(results, info=True) }} --{{ return([]) }}

{% endmacro %}

--------------------- next get_order_maxdate

{% macro get_order_maxdate() -%}

{% set max_date_query %}
select max(orderdate)
from {{ ref("fact_orders")}} 
--order by 1
{% endset %}

{% set results = run_query(max_date_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0][0] %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{%- endmacro %}

----------------------- next get_order_mindate

{% macro get_order_mindate() -%}

{% set min_date_query %}
select min(orderdate)
from {{ ref("fact_orders")}} 
--order by 1
{% endset %}

{% set results = run_query(min_date_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0][0] %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{%- endmacro %}