import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, DoubleType
from delta.tables import DeltaTable

load_dotenv()

spark = (
    SparkSession.builder
    .appName("Silver Transform")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

BRONZE = "s3a://ec-dwh-main/bronze/delta"
SILVER = "s3a://ec-dwh-main/silver"


def read_bronze(table: str):
    return spark.read.format("delta").load(f"{BRONZE}/{table}")


def write_silver_merge(df, table: str, pk_cols: list):
    path = f"{SILVER}/{table}"
    merge_condition = " AND ".join(
        [f"existing.{k} = incoming.{k}" for k in pk_cols]
    )

    if DeltaTable.isDeltaTable(spark, path):
        # 増分: 既存行UPDATE + 新規行INSERT 
        dt = DeltaTable.forPath(spark, path)
        (
            dt.alias("existing")
            .merge(df.alias("incoming"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[Silver] {table}: merged")
    else:
        # 初回: Deltaテーブルを新規作成 
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        )
        print(f"[Silver] {table}: {df.count()} rows created (initial load)")


# ── 1. orders ───────────────────────────────────────────────────────────────
def transform_orders():
    df = read_bronze("olist_orders_dataset")

    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    df = (
        df
        .dropDuplicates(["order_id"])
        .dropna(subset=["order_id", "customer_id", "order_status"])
    )

    for col in date_cols:
        df = df.withColumn(col, F.to_timestamp(col))

    df = df.filter(
        F.col("order_approved_at").isNull() |
        (F.col("order_approved_at") >= F.col("order_purchase_timestamp"))
    )

    write_silver_merge(df, "orders", ["order_id"])


# ── 2. order_items ──────────────────────────────────────────────────────────
def transform_order_items():
    df = read_bronze("olist_order_items_dataset")

    df = (
        df
        .dropDuplicates(["order_id", "order_item_id"])
        .dropna(subset=["order_id", "product_id", "seller_id"])
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
        .filter(F.col("price") > 0)
        .filter(F.col("freight_value") >= 0)
    )

    write_silver_merge(df, "order_items", ["order_id", "order_item_id"])


# ── 3. customers ────────────────────────────────────────────────────────────
def transform_customers():
    df = read_bronze("olist_customers_dataset")

    df = (
        df
        .dropDuplicates(["customer_id"])
        .dropna(subset=["customer_id", "customer_unique_id"])
        .withColumn(
            "customer_zip_code_prefix",
            F.lpad(F.col("customer_zip_code_prefix").cast("string"), 5, "0")
        )
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
    )

    write_silver_merge(df, "customers", ["customer_id"])


# ── 4. products ─────────────────────────────────────────────────────────────
def transform_products():
    df = read_bronze("olist_products_dataset")

    df = (
        df
        .dropDuplicates(["product_id"])
        .dropna(subset=["product_id"])
        .withColumn("product_name_lenght",
                    F.col("product_name_lenght").cast(IntegerType()))
        .withColumn("product_description_lenght",
                    F.col("product_description_lenght").cast(IntegerType()))
        .withColumn("product_photos_qty",
                    F.col("product_photos_qty").cast(IntegerType()))
        .withColumn("product_weight_g",
                    F.col("product_weight_g").cast(FloatType()))
        .withColumn("product_length_cm",
                    F.col("product_length_cm").cast(FloatType()))
        .withColumn("product_height_cm",
                    F.col("product_height_cm").cast(FloatType()))
        .withColumn("product_width_cm",
                    F.col("product_width_cm").cast(FloatType()))
        .withColumn(
            "product_category_name",
            F.coalesce(F.col("product_category_name"), F.lit("unknown"))
        )
        .withColumn("product_weight_g",
                    F.when(F.col("product_weight_g") > 0,
                           F.col("product_weight_g")))
    )

    write_silver_merge(df, "products", ["product_id"])


# ── 5. sellers ──────────────────────────────────────────────────────────────
def transform_sellers():
    df = read_bronze("olist_sellers_dataset")

    df = (
        df
        .dropDuplicates(["seller_id"])
        .dropna(subset=["seller_id"])
        .withColumn(
            "seller_zip_code_prefix",
            F.lpad(F.col("seller_zip_code_prefix").cast("string"), 5, "0")
        )
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
        .withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
    )

    write_silver_merge(df, "sellers", ["seller_id"])


# ── 6. order_payments ───────────────────────────────────────────────────────
def transform_order_payments():
    df = read_bronze("olist_order_payments_dataset")

    df = (
        df
        .dropDuplicates(["order_id", "payment_sequential"])
        .dropna(subset=["order_id", "payment_type"])
        .withColumn("payment_sequential",
                    F.col("payment_sequential").cast(IntegerType()))
        .withColumn("payment_installments",
                    F.col("payment_installments").cast(IntegerType()))
        .withColumn("payment_value",
                    F.col("payment_value").cast(DoubleType()))
        .filter(F.col("payment_value") > 0)
    )

    write_silver_merge(df, "order_payments", ["order_id", "payment_sequential"])


# ── 7. order_reviews ────────────────────────────────────────────────────────
def transform_order_reviews():
    df = read_bronze("olist_order_reviews_dataset")

    df = (
        df
        .dropDuplicates(["review_id"])
        .dropna(subset=["review_id", "order_id", "review_score"])
        .withColumn("review_score",
                    F.col("review_score").cast(IntegerType()))
        .withColumn("review_creation_date",
                    F.to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp",
                    F.to_timestamp("review_answer_timestamp"))
        .filter(F.col("review_score").between(1, 5))
        .withColumn("review_comment_title",
                    F.nullif(F.trim(F.col("review_comment_title")), F.lit("")))
        .withColumn("review_comment_message",
                    F.nullif(F.trim(F.col("review_comment_message")), F.lit("")))
        .withColumn("has_comment",
                    F.col("review_comment_message").isNotNull())
    )

    write_silver_merge(df, "order_reviews", ["review_id"])


# ── 8. geolocation ──────────────────────────────────────────────────────────
def transform_geolocation():
    df = read_bronze("olist_geolocation_dataset")

    df = (
        df
        .dropDuplicates(["geolocation_zip_code_prefix",
                         "geolocation_lat",
                         "geolocation_lng"])
        .dropna(subset=["geolocation_zip_code_prefix",
                        "geolocation_lat",
                        "geolocation_lng"])
        .withColumn("geolocation_lat",
                    F.col("geolocation_lat").cast(DoubleType()))
        .withColumn("geolocation_lng",
                    F.col("geolocation_lng").cast(DoubleType()))
        .withColumn(
            "geolocation_zip_code_prefix",
            F.lpad(F.col("geolocation_zip_code_prefix").cast("string"), 5, "0")
        )
        .filter(F.col("geolocation_lat").between(-35.0, 5.5))
        .filter(F.col("geolocation_lng").between(-74.0, -34.0))
        .groupBy("geolocation_zip_code_prefix", "geolocation_city",
                 "geolocation_state")
        .agg(
            F.percentile_approx("geolocation_lat", 0.5).alias("geolocation_lat"),
            F.percentile_approx("geolocation_lng", 0.5).alias("geolocation_lng"),
        )
    )

    # geolocationはzip+city+stateの複合PKで管理
    write_silver_merge(df, "geolocation",
                       ["geolocation_zip_code_prefix",
                        "geolocation_city",
                        "geolocation_state"])


# ── 9. product_category_name_translation ────────────────────────────────────
def transform_category_translation():
    df = read_bronze("product_category_name_translation")

    df = (
        df
        .dropDuplicates(["product_category_name"])
        .dropna()
        .withColumn("product_category_name",
                    F.lower(F.trim(F.col("product_category_name"))))
        .withColumn("product_category_name_english",
                    F.lower(F.trim(F.col("product_category_name_english"))))
    )

    write_silver_merge(df, "product_category_name_translation",
                       ["product_category_name"])


if __name__ == "__main__":
    print("=== Silver Transform Start ===")
    transform_orders()
    transform_order_items()
    transform_customers()
    transform_products()
    transform_sellers()
    transform_order_payments()
    transform_order_reviews()
    transform_geolocation()
    transform_category_translation()
    print("=== Silver done. ===")
    spark.stop()