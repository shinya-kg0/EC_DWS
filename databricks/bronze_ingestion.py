import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

BUCKET       = 'ec-dwh-main'
S3_INPUT     = f's3a://{BUCKET}/raw/olist/'
S3_OUTPUT    = f's3a://{BUCKET}/bronze/delta/'

spark = SparkSession.builder \
    .appName("bronze_ingestion") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

TABLES = [
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_customers_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_geolocation_dataset",
    "product_category_name_translation",
]

for tbl in TABLES:
    df = spark.read.csv(
        f"{S3_INPUT}{tbl}.csv",
        header=True,
        inferSchema=False
    )
    (df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{S3_OUTPUT}{tbl}/"))
    print(f"[Bronze] {tbl}: {df.count()} rows to s3")

spark.stop()
print("Bronze done.")