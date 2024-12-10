{{config(materialized = 'view', schema = 'reporting')}}

SELECT 
cust.COMPANYNAME, 
cust.CONTACTNAME, 
MIN(ORDERDATE) as FIRST_ORDERDATE, 
MAX(ORDERDATE) as RECENT_ORDERDATE,
COUNT(*) as ORDER_COUNT, 
SUM(ORD.LINESALEAMOUNT) as SALES, 
AVG(MERGIN) as AVG_MARGIN

FROM {{ref('fct_orders')}} as ord
INNER JOIN {{ref('dim_customers')}} as cust ON cust.customerid = ord.customerid
GROUP BY COMPANYNAME, CONTACTNAME
order by sales desc