{{config(materialized = 'table', schema ='transforming' )}}

select 
o.OrderID,
od.LineNo,
o.CustomerID,
o.employeeid,
o.shipperid,
od.ProductID,
o.Freight,
od.Quantity,
od.UnitPrice,
od.discount,
o.orderdate,
(od.UnitPrice * od.Quantity) * (od.discount) as linesaleamount,
p.UnitCost * od.Quantity as costofgoodssold,
((od.UnitPrice * od.Quantity)) * (1-od.discount) - (p.UnitCost * od.Quantity) as mergin 

from
{{ref('stg_orders')}} as o 
inner join {{ref("stg_orderdetails")}} as od 
on o.OrderID = od.OrderID
inner join {{ref('stg_products')}} as p
on od.ProductID = p.ProductID