import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()

spark = (
    SparkSession.builder
    .appName("Gold Aggregation")
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

SILVER = "s3a://ec-dwh-main/silver"
GOLD   = "s3a://ec-dwh-main/gold"


def read_silver(table: str):
    return spark.read.format("delta").load(f"{SILVER}/{table}")


def write_gold(df, table: str):
    path = f"{GOLD}/{table}"
    df.write.mode("overwrite").parquet(path)
    print(f"[Gold] {table}: {df.count()} rows → s3")


def build_fact_orders():
    orders   = read_silver("orders")
    customers = read_silver("customers")
    items    = read_silver("order_items")
    payments = read_silver("order_payments")
    reviews  = read_silver("order_reviews")

    payments_agg = (
        payments
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.first("payment_type").alias("primary_payment_type"),
            F.max("payment_installments").alias("max_installments"),
        )
    )

    items_agg = (
        items
        .groupBy("order_id")
        .agg(
            F.sum("price").alias("total_item_price"),
            F.sum("freight_value").alias("total_freight_value"),
            F.count("order_item_id").alias("item_count"),
        )
    )

    review_window = Window.partitionBy("order_id").orderBy(
        F.desc("review_creation_date")
    )
    reviews_dedup = (
        reviews
        .withColumn("rn", F.row_number().over(review_window))
        .filter(F.col("rn") == 1)
        .select("order_id", "review_score", "has_comment")
    )

    orders_enriched = orders.withColumn(
        "is_delayed",
        F.when(
            F.col("order_delivered_customer_date").isNotNull() &
            (F.col("order_delivered_customer_date") >
            F.col("order_estimated_delivery_date")),
            True
        ).otherwise(False)
    ).withColumn(
        "delivery_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull(),
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp")
            )
        )
    )

    fact = (
        orders_enriched
        .join(customers.select(
                "customer_id", "customer_unique_id",
                "customer_state", "customer_city"),
                "customer_id", "left")
        .join(payments_agg, "order_id", "left")
        .join(items_agg,    "order_id", "left")
        .join(reviews_dedup,"order_id", "left")
        .select(
            "order_id",
            "customer_id",
            "customer_unique_id",
            "customer_state",
            "customer_city",
            "order_status",
            "order_purchase_timestamp",
            F.to_date("order_purchase_timestamp").alias("order_date"),
            F.year("order_purchase_timestamp").alias("order_year"),
            F.month("order_purchase_timestamp").alias("order_month"),
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "is_delayed",
            "delivery_days",
            "total_payment_value",
            "primary_payment_type",
            "max_installments",
            "total_item_price",
            "total_freight_value",
            "item_count",
            "review_score",
            "has_comment",
        )
    )

    write_gold(fact, "fact_orders")


def build_agg_daily_sales():
    fact = spark.read.parquet(f"{GOLD}/fact_orders")

    daily = (
        fact
        .filter(F.col("order_status") == "delivered")
        .groupBy("order_date", "order_year", "order_month")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_payment_value").alias("revenue"),
            F.avg("total_payment_value").alias("avg_order_value"),
            F.sum("item_count").alias("total_items_sold"),
            F.avg("review_score").alias("avg_review_score"),
            F.sum(F.when(F.col("is_delayed"), 1).otherwise(0)).alias("delayed_count"),
        )
        .withColumn(
            "delay_rate",
            F.round(F.col("delayed_count") / F.col("order_count") * 100, 2)
        )
        .orderBy("order_date")
    )

    write_gold(daily, "agg_daily_sales")


def build_agg_category_sales():
    items    = read_silver("order_items")
    orders   = read_silver("orders")
    products = read_silver("products")
    trans    = read_silver("product_category_name_translation")
    reviews  = read_silver("order_reviews")

    products_en = (
        products
        .join(trans, "product_category_name", "left")
        .withColumn(
            "category_en",
            F.coalesce(
                F.col("product_category_name_english"),
                F.col("product_category_name")
            )
        )
        .select("product_id", "product_category_name", "category_en")
    )

    delivered_orders = orders.filter(F.col("order_status") == "delivered") \
                            .select("order_id", "order_purchase_timestamp")

    review_avg = reviews.groupBy("order_id").agg(
        F.avg("review_score").alias("review_score")
    )

    category_sales = (
        items
        .join(delivered_orders, "order_id", "inner")
        .join(products_en, "product_id", "left")
        .join(review_avg, "order_id", "left")
        .groupBy("product_category_name", "category_en")
        .agg(
            F.count("order_item_id").alias("total_items_sold"),
            F.countDistinct("order_id").alias("order_count"),
            F.sum("price").alias("revenue"),
            F.avg("price").alias("avg_price"),
            F.avg("freight_value").alias("avg_freight"),
            F.avg("review_score").alias("avg_review_score"),
            F.countDistinct("product_id").alias("unique_products"),
        )
        .orderBy(F.desc("revenue"))
    )

    write_gold(category_sales, "agg_category_sales")


def build_agg_seller_performance():
    items   = read_silver("order_items")
    orders  = read_silver("orders")
    sellers = read_silver("sellers")
    reviews = read_silver("order_reviews")

    delivered = orders.filter(F.col("order_status") == "delivered").select(
        "order_id",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ).withColumn(
        "is_delayed",
        F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date")
    )

    review_avg = reviews.groupBy("order_id").agg(
        F.avg("review_score").alias("review_score")
    )

    seller_perf = (
        items
        .join(delivered, "order_id", "inner")
        .join(review_avg, "order_id", "left")
        .groupBy("seller_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.count("order_item_id").alias("total_items_sold"),
            F.sum("price").alias("revenue"),
            F.avg("price").alias("avg_price"),
            F.avg("review_score").alias("avg_review_score"),
            F.sum(F.when(F.col("is_delayed"), 1).otherwise(0)).alias("delayed_orders"),
            F.avg(
                F.datediff(
                    F.col("order_delivered_customer_date"),
                    F.col("order_purchase_timestamp")
                )
            ).alias("avg_delivery_days"),
        )
        .withColumn(
            "delay_rate",
            F.round(F.col("delayed_orders") / F.col("order_count") * 100, 2)
        )
        .join(
            sellers.select("seller_id", "seller_state", "seller_city"),
            "seller_id", "left"
        )
        .orderBy(F.desc("revenue"))
    )

    write_gold(seller_perf, "agg_seller_performance")


def build_reviews_for_tttc():
    reviews  = read_silver("order_reviews")
    items    = read_silver("order_items")
    products = read_silver("products")
    trans    = read_silver("product_category_name_translation")
    orders   = read_silver("orders")

    products_en = (
        products
        .join(trans, "product_category_name", "left")
        .withColumn(
            "category_en",
            F.coalesce(
                F.col("product_category_name_english"),
                F.col("product_category_name")
            )
        )
        .select("product_id", "product_category_name", "category_en")
    )

    item_category = (
        items
        .join(products_en, "product_id", "left")
        .groupBy("order_id")
        .agg(F.first("product_category_name").alias("product_category_name"),
            F.first("category_en").alias("category_en"))
    )

    tttc = (
        reviews
        .filter(F.col("has_comment") == True)
        .join(
            orders.select("order_id", "order_status",
                        "order_purchase_timestamp"),
                        "order_id", "left"
        )
        .join(item_category, "order_id", "left")
        .select(
            "review_id",
            "order_id",
            "review_score",
            F.col("review_comment_message").alias("text"),
            "review_comment_title",
            "review_creation_date",
            "order_status",
            F.to_date("order_purchase_timestamp").alias("order_date"),
            "product_category_name",
            "category_en",
            F.when(F.col("review_score") >= 4, "positive")
            .when(F.col("review_score") == 3, "neutral")
            .otherwise("negative")
            .alias("sentiment_label"),
        )
        .orderBy("review_creation_date")
    )

    write_gold(tttc, "reviews_for_tttc")


if __name__ == "__main__":
    print("=== Gold Aggregation Start ===")
    build_fact_orders()
    build_agg_daily_sales()
    build_agg_category_sales()
    build_agg_seller_performance()
    build_reviews_for_tttc()
    print("=== Gold done. ===")
    spark.stop()