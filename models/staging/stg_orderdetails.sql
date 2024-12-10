{{config(materialized = 'incremental',unique_key=['orderid','lineno'], schema = env_var('DBT_STAGESCHEMA', 'STAGING_DEV'))}}

select 
od.*,
o.OrderDate
from
{{source("qwt_raw",'OrderDetails')}} as od
inner join {{source("qwt_raw",'ORDERS')}} as o
on od.orderid = o.orderid

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where o.OrderDate > (select max(OrderDate) from {{ this }}) --this refers current model

{% endif %}