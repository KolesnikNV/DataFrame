from utils import (
    create_spark_session,
    create_products_data,
    create_categories_data,
)
from data_processing import join_products_and_categories

spark = create_spark_session("ProductCategoryPairs")

products_data = create_products_data(spark)
categories_data = create_categories_data(spark)

result_df = join_products_and_categories(products_data, categories_data)

result_df.show()

spark.stop()
