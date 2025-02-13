
def model(dbt, session):

    customers_df = dbt.source("raw_qwt","raw_products")

    return customers_df