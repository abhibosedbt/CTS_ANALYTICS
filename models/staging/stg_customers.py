def model(dbt, session):
    stg_cust_df = dbt.source("qwt_raw", "CUSTOMERS")
    return stg_cust_df