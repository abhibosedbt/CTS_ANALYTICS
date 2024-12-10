{{config(materialized = 'table', schema = env_var('DBT_STAGESCHEMA', 'STAGING_DEV'))}}

select 
OrderID,
LineNo,
ShipperID,
CustomerID,
ProductID,
EmployeeID,
split_part(ShipmentDate,' ',0)::date as ShipmentDate,
Status

from
{{source('qwt_raw','shipments')}}
