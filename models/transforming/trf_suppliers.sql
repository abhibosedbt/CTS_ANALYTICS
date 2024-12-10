{{config(materialized = 'table', schema = 'transforming')}}

select 

GET(XMLGET(supplier_info, 'SupplierID'), '$') as SupplierID,
GET(XMLGET(supplier_info, 'CompanyName'), '$')::VARCHAR as CompanyName,
GET(XMLGET(supplier_info, 'ContactName'), '$')::VARCHAR as ContactName,
GET(XMLGET(supplier_info, 'Address'), '$')::VARCHAR as Address,
GET(XMLGET(supplier_info, 'City'), '$')::VARCHAR as City,
GET(XMLGET(supplier_info, 'PostalCode'), '$')::VARCHAR as PostalCode,
GET(XMLGET(supplier_info, 'Country'), '$')::VARCHAR as Country,
GET(XMLGET(supplier_info, 'Phone'), '$')::VARCHAR as Phone,
GET(XMLGET(supplier_info, 'Fax'), '$')::VARCHAR as Fax

from
{{ref('stg_suppliers')}}