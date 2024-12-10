{{config(materialized = 'view', schema ='reporting')}}

select 
cust.companyname,
cust.contactname,
cust.city,
sum(ord.linesaleamount) sales,
sum(ord.quantity) quantity,
avg(ord.mergin) mergin
from
{{ref('dim_customers')}} as cust --model name & case sensitive
inner join {{ref('fct_orders')}} as ord  --model name & case sensitive

on ord.customerid = cust.customerid
--where cust.city='London'
where cust.city= {{var('vcity',"'London'")}} --variable defined
group by
cust.companyname,
cust.contactname,
cust.city
order by sales desc 