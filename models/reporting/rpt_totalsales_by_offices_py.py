import snowflake.snowpark.functions as F
import pandas as pd

def model(dbt, session):

    dbt.config(materialized = 'table', schema='reporting_dev')
    
    dim_customers_df = dbt.ref('dim_customers')
    fct_orders_df  = dbt.ref('fact_orders')

    final_df=(
        fct_orders_df.join(dim_customers_df,fct_orders_df.customerid==dim_customers_df.customerid,'inner')
        .select(dim_customers_df.companyname.alias('companyname'),
                dim_customers_df.contactname.alias('contactname'),
                fct_orders_df.orderdate.alias('orderdate'),                
                fct_orders_df.orderid.alias('orderid'),                   
                fct_orders_df.linesalesamount.alias('linesalesamount'),
                fct_orders_df.margin.alias('margin'),                
        )
    )

    return final_df