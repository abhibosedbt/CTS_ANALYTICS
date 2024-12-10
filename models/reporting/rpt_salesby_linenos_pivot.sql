{{config(materialized = 'view', schema ='reporting')}}

{% set  LineNo = get_linenos() %} --macro is called

select
ORDERID,

{% for lno in LineNo %}
sum(case when lineno = {{lno}} then linesaleamount end) as lineno{{lno}}_sales,
{% endfor %}
sum(linesaleamount) as total_sales
from {{ ref('fct_orders') }}
group by 1
