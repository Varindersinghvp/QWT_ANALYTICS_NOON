
def model(dbt, session):

    customers_df = dbt.source("raw_qwt","raw_customers")

    return customers_df