{{config(materialized = 'table', schema = env_var('DBT_STAGESCHEMA', 'STAGING_DEV'))}}

select 
EmpID,
LastName,
FirstName,
Title,
HireDate,
Office,
Extension,
ReportsTo,
YearSalary
from 
--qwtanalytics.raw_dev.customers
{{source('qwt_raw','employees')}}