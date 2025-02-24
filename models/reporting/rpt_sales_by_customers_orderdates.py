import snowflake.snowpark.functions as F
import pandas as pd
import holidays

def avgordervalue(nooforders,totalvalue):

    return totalvalue/nooforders

def is_holiday(holiday_date):
    # Chez Jaffle
    french_holidays = holidays.France()
    is_holiday = (holiday_date in french_holidays)
    return is_holiday

def model(dbt, session):

    dbt.config(materialized = 'table', schema='reporting_dev', packages=["holidays"], pre_hook='use warehouse PYTHON_MODELS_WH;')
    
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
                orders_agg_df.Avg_Margin.alias('Avg_Margin')                                                            
        )
    )

    # Adding one more new column AVG_ORDER_AMOUNT
    final_df = final_df.withColumn('AVG_ORDER_AMOUNT',avgordervalue(final_df["Total_orders"],final_df["Totalsales"]))

    # Condition to remove Null values
    final_df = final_df.filter(F.col("FIRST_ORDER").isNotNull())

    final_df = final_df.to_pandas()

    # Adding one more new column IS_HOLIDAY
    final_df["IS_HOLIDAY"] = final_df["FIRST_ORDER"].apply(is_holiday)

    return final_df

