{{config(materialized = 'table', schema = 'transforming')}}

select 
c.CustomerID,
c.CompanyName,
c.ContactName,
c.City,
c.Country,
d.divisionname,
c.Address,
c.Fax,
c.PostalCode,
c.Phone,
IFF(c.state ='','NA', c.state) as statename
from 
{{ref("stg_customers")}} as c 
inner join
{{ref("lkp_divisions")}} as d 
on c.DivisionID = d.DivisionID