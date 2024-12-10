{{config(materialized = 'table', schema = env_var('DBT_STAGESCHEMA', 'STAGING_DEV'), transient = false, pre_hook = "use warehouse loading_wh;",
    post_hook = "create or replace table stg_products_clone clone stg_products;"
    )}}

select 
*
from 

{{source('qwt_raw','Products')}}