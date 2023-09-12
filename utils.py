from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def create_spark_session(app_name="MySparkApp"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def create_products_data(spark):
    products_data = [
        (1, "Product A"),
        (2, "Product B"),
        (3, "Product C"),
        (4, "Product D"),
    ]

    schema = StructType(
        [
            StructField("ProductID", IntegerType(), False),
            StructField("ProductName", StringType(), False),
        ]
    )

    return spark.createDataFrame(products_data, schema=schema)


def create_categories_data(spark):
    categories_data = [
        (1, "Category X"),
        (2, "Category Y"),
        (3, "Category Z"),
    ]

    schema = StructType(
        [
            StructField("ProductID", IntegerType(), False),
            StructField("CategoryName", StringType(), False),
        ]
    )

    return spark.createDataFrame(categories_data, schema=schema)


def join_products_and_categories(products_df, categories_df):
    return products_df.join(categories_df, "ProductID", "left").select(
        "ProductName",
        F.when(F.col("CategoryName").isNull(), "No Category")
        .otherwise(F.col("CategoryName"))
        .alias("CategoryName"),
    )
