import snowflake.snowpark.functions as F 
import pandas as pd
import holidays

def avg_sale(x,y):
    return y/x

#use of pandas holidays packages
def is_holiday(date_col):
    french_holidays = holidays.France() #this gives you the list of holidays
    is_holiday = (date_col in french_holidays)
    return is_holiday

def model(dbt, session):
    dbt.config(materialized = "table", schema = 'reporting', packages = ["holidays"]) #add in config
    
    dim_customers_df = dbt.ref('dim_customers')
    fct_orders_df = dbt.ref('fct_orders')

    cust_orders_df =(
                        fct_orders_df
                        .group_by('customerid')
                        .agg
                        (
                        F.min(F.col('orderdate')).alias('first_orderdate'),
                        F.max(F.col('orderdate')).alias('recent_orderdate'),
                        F.count(F.col('orderid')).alias('total_orders'),
                        F.sum(F.col('linesaleamount')).alias('total_sales'),
                        F.avg(F.col('mergin')).alias('avg_margin')
                        )
                    )

    final_df = (
                dim_customers_df 
                .join(cust_orders_df, cust_orders_df.customerid == dim_customers_df.customerid, 'left')
                .select(
                dim_customers_df.companyname.alias('companyname'),
                dim_customers_df.contactname.alias('contactname'),
                cust_orders_df.first_orderdate.alias('first_orderdate'),
                cust_orders_df.recent_orderdate.alias('recent_orderdate'),
                cust_orders_df.total_orders.alias('total_orders'),
                cust_orders_df.total_sales.alias('total_sales'),
                cust_orders_df.avg_margin.alias('avg_margin')
                        )
                )
    
    final_df = final_df.withColumn("avg_salevalue", avg_sale(final_df["total_orders"], final_df["total_sales"]))
    final_df = final_df.filter(F.col("first_orderdate").isNotNull()) #remove null in Snowpark
    final_df = final_df.to_pandas() #convert in Pandas

    final_df ["IS_FIRST_ORDER_DATE"] = final_df["FIRST_ORDERDATE"].apply(is_holiday)

    return final_df