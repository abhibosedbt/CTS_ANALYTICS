import snowflake.snowpark.functions as F 

def model(dbt, session):
    dbt.config(materialized = "incremental", unique_key = 'orderid')
    df = dbt.source("qwt_raw","ORDERS")
    
    if dbt.is_incremental:
        # only new rows compared to max in current table
        max_orderdate = f"select max(ORDERDATE) from {dbt.this}"
        df = df.filter(df.ORDERDATE > session.sql(max_orderdate).collect()[0][0])

    return df