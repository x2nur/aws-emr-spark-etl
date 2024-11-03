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
        print(f"Usage: {sys.argv[0]} <path_to_product.csv>", file=sys.stderr)
        exit(-1)

    product_path = sys.argv[1]
    products_tab = "dev.lakeh.dim_products"

    spark: SQL.SparkSession = (
        SQL.SparkSession
        .builder
        .appName("Product-ETL")
        .getOrCreate()
    )

    rd_product: SQL.DataFrameReader = make_csv_reader(spark)
    df_product = rd_product.load(product_path)

    hash_columns = [
        "id", 
        "price",
        "title",
        "category",
        "vendor"
    ]

    cat_cols = []
    for _, name in enumerate(hash_columns[:-1]):
        cat_cols += [F.col(name), F.lit(":")]
    cat_cols.append(F.col(hash_columns[-1]))

    # typify columns
    df_product = (
        df_product.withColumns({
            "product_sk": F.md5(F.concat(*cat_cols)),
            "product_id": F.col("id").cast(T.LongType()),
            "price": F.col("price").cast(T.DecimalType()),
            "created_at": F.to_timestamp("created_at"),
            "is_current": F.lit(1)
        })
        .select(
            "product_sk",
            "product_id",
            "price",
            "title",
            "category",
            "vendor",
            "created_at",
            "is_current",
        )
        .orderBy("product_sk")
    )

    if not spark.catalog.tableExists(products_tab):
        (df_product
        .writeTo(products_tab)
        .using("iceberg")
        .partitionedBy(F.bucket(20, "product_sk"))
        .create() )
        
        return 
            
    # load product from dwh
    df_product_dwh: SQL.DataFrame = spark.table(products_tab)

    # new or updated
    df_product_ins: SQL.DataFrame = F.broadcast(df_product).join(df_product_dwh, 'product_sk', 'leftanti')

    # all recs from dwh w/ updated is_current field
    df_product_upd: SQL.DataFrame = (
        df_product_dwh.alias("l")
        .join(F.broadcast(df_product_ins).alias("r"), "product_id", "leftsemi")
        .select(
            [F.col(c) for c in df_product_dwh.columns if c != 'is_current']
            + [F.lit(0).alias("is_current")]
        )
    )

    # all others from dwh
    df_product_others: SQL.DataFrame = (
        df_product_dwh.join(df_product, "product_id", "leftanti")
    )

    # final df
    df_product_fin: SQL.DataFrame = (
        df_product_upd
        .union(df_product_ins)
        .union(df_product_others)
        .orderBy("product_sk")
    )

    (
        df_product_fin
        .writeTo(products_tab)
        .using("iceberg")
        .partitionedBy(F.bucket(20, "product_sk"))
        .replace()
    )


if __name__ == "__main__":
    main()