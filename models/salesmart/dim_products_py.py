
def model(dbt, session):

    customers_df = dbt.ref("trf_products")

    return customers_df