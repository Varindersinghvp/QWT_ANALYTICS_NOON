import snowflake.snowpark.functions as F
import pandas as pd

def avgordervalue(nooforders,totalvalue):

    return totalvalue/nooforders

def model(dbt, session):

    dbt.config(materialized = 'table', schema='reporting_dev')
    
    dim_customers_df = dbt.ref('dim_customers')
    fct_orders_df  = dbt.ref('fact_orders')
    
    orders_agg_df=(fct_orders_df
           .group_by('customerid')
       .agg(
           F.min(F.col('orderdate')).alias('first_order'),
           F.max(F.col('orderdate')).alias('most_recent_order'),
           F.countDistinct(F.col('orderid')).alias('Total_orders'),
           F.sum(F.col('quantity')).alias('Total_quantity'),           
           F.sum(F.col('LINESALESAMOUNT')).alias('Totalsales'),
           F.avg(F.col('margin')).alias('Avg_Margin')
       )
   )

    final_df=(
        orders_agg_df.join(dim_customers_df,orders_agg_df.customerid==dim_customers_df.customerid,'left')
        .select(dim_customers_df.companyname.alias('companyname'),
                dim_customers_df.contactname.alias('contactname'),
                orders_agg_df.first_order.alias('first_order'),                
                orders_agg_df.most_recent_order.alias('most_recent_order'),                   
                orders_agg_df.Total_orders.alias('Total_orders'),
                orders_agg_df.Total_quantity.alias('Total_quantity'),                
                orders_agg_df.Totalsales.alias('Totalsales'), 
                orders_agg_df.Avg_Margin.alias('Avg_Margin'),
                (orders_agg_df.Totalsales/orders_agg_df.Total_orders).alias('avgorderpaisa')                                                                           
        )
    )

    return final_df

