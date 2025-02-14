import snowflake.snowpark.functions as F
import pandas as pd

def model(dbt, session):

    dbt.config(materialized = 'incremental', unique_key='OrderID')
    
    df = dbt.source("raw_qwt", "raw_orders")

    if dbt.is_incremental:
        max_from_this = f"select max(orderdate) from {dbt.this}"
        df = df.filter(df.orderdate > session.sql(max_from_this).collect()[0][0])

    return df