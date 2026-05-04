import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

load_dotenv()

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

BUCKET    = 'ec-dwh-main'
S3_INPUT  = f's3a://{BUCKET}/raw/olist/'
S3_OUTPUT = f's3a://{BUCKET}/bronze/delta/'

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

PRIMARY_KEYS = {
    "olist_orders_dataset":                  ["order_id"],
    "olist_order_items_dataset":             ["order_id", "order_item_id"],
    "olist_customers_dataset":               ["customer_id"],
    "olist_products_dataset":                ["product_id"],
    "olist_sellers_dataset":                 ["seller_id"],
    "olist_order_payments_dataset":          ["order_id", "payment_sequential"],
    "olist_order_reviews_dataset":           ["review_id"],
    "olist_geolocation_dataset":             ["geolocation_zip_code_prefix",
                                              "geolocation_lat",
                                              "geolocation_lng"],
    "product_category_name_translation":     ["product_category_name"],
}

TABLES = list(PRIMARY_KEYS.keys())


def ingest_table(tbl: str, pks: list[str]):
    path = f"{S3_OUTPUT}{tbl}/"
    df = spark.read.csv(
        f"{S3_INPUT}{tbl}.csv",
        header=True,
        inferSchema=False,
    )

    if DeltaTable.isDeltaTable(spark, path):
        # 増分: 新規行のみINSERT
        merge_condition = " AND ".join(
            [f"existing.{k} = incoming.{k}" for k in pks]
        )
        dt = DeltaTable.forPath(spark, path)
        (
            dt.alias("existing")
            .merge(df.alias("incoming"), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[Bronze] {tbl}: merged (new rows only)")
    else:
        # 初回: Deltaテーブルを新規作成
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(path)
        )
        print(f"[Bronze] {tbl}: {df.count()} rows created (initial load)")


for tbl in TABLES:
    ingest_table(tbl, PRIMARY_KEYS[tbl])

spark.stop()
print("Bronze done.")