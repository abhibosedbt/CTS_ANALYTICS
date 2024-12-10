{{config(materialized = 'view', schema = 'salesmart')}}

{% set min_orderdate = get_min_orderdate() %} {# comes from macros #}
{% set max_orderdate = get_max_orderdate() %} 

{# under macros in dbt_packages - you can hardcode below parameters #}
{{dbt_date.get_date_dimension(min_orderdate,max_orderdate)}} 