import sys
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql as SQL


def make_csv_reader(spark: SQL.SparkSession) -> SQL.DataFrameReader:
    csv_reader = (
        spark
        .read
        .format('csv')
        .option("inferSchema", "true")
        .option("header", "true")
    )
    return csv_reader


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <path_to_orders.csv>", file=sys.stderr)
        exit(-1)

    order_path = sys.argv[1]

    tab_orders = "dev.lakeh.fact_orders"
    tab_products = "dev.lakeh.dim_products"
    tab_users = "dev.lakeh.dim_users"

    spark: SQL.SparkSession = (
        SQL.SparkSession
        .builder
        .appName("Order-ETL")
        .getOrCreate()
    )

    df_order: SQL.DataFrame = make_csv_reader(spark).load(order_path)

    # typify
    df_order = (
        df_order.withColumns({
            "order_sk": F.md5(F.col("id").cast(T.StringType())),
            "order_id": F.col("id").cast(T.LongType()),
            "user_id": F.col("user_id").cast(T.LongType()),
            "product_id": F.col("product_id").cast(T.LongType()),
            "discount": F.col("discount").cast(T.DecimalType(4,2)),
            "quantity": F.col("quantity").cast(T.IntegerType()),
            "created_at": F.to_timestamp("created_at"),
        }).drop("id")
    )

    df_product_dwh: SQL.DataFrame = spark.table(tab_products)
    df_product_dwh = df_product_dwh.where(F.col('is_current') == 1).select("product_sk", "product_id")

    df_users_dwh: SQL.DataFrame = spark.table(tab_users).select("user_sk", "user_id")

    df_order_fin: SQL.DataFrame = (
        df_order
        .withColumn("date_sk", F.date_format("created_at", "yyyyMMdd").cast(T.IntegerType())) # cdim_calendar
        .drop("created_at")
        .join(df_users_dwh, "user_id", "inner")
        .drop("user_id")
        .join(df_product_dwh, "product_id", "inner")
        .drop("product_id")
        .select(
            "order_sk",
            "date_sk",
            "user_sk",
            "product_sk",
            "order_id",
            "discount",
            "quantity",
        )
        .orderBy("date_sk")
    )

    writer = df_order_fin.writeTo(tab_orders)

    if spark.catalog.tableExists(tab_orders):
        writer.append()
    else:
        (writer
        .using("iceberg")
        .partitionedBy(F.col("date_sk"))
        .create() )


if __name__ == "__main__":
    main()