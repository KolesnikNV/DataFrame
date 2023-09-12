from spark_session import create_spark_session


def join_products_and_categories(products_df, categories_df):
    products_df.createOrReplaceTempView("products")
    categories_df.createOrReplaceTempView("categories")
    spark = create_spark_session()
    query = """
        SELECT p.ProductName, c.CategoryName
        FROM products p
        LEFT JOIN categories c
        ON p.ProductID = c.ProductID
    """
    return spark.sql(query)
