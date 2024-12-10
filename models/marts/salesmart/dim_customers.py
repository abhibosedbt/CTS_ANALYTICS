def model(dbt, session):
    dbt.config(
        materialized="table",
        schema = 'salesmart'        
    )
    dim_cust_df = dbt.ref('trf_customers')
    return dim_cust_df