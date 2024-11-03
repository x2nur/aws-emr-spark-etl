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


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <path_to_user.csv>", file=sys.stderr)
        exit(-1)

    spark: SQL.SparkSession = (
        SQL.SparkSession
        .builder
        .appName("User-ETL")
        .getOrCreate()
    )

    user_data_path = sys.argv[1]
    users_tab = "dev.lakeh.dim_users"

    users_csv_rd = make_csv_reader(spark)
    users_df = users_csv_rd.load(user_data_path)

    users_df = (
        users_df
        .withColumn("user_sk", F.md5(F.col("id").cast(T.StringType())))
        .withColumn("user_id",  F.col("id").cast(T.LongType()))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("birth_date", F.to_date("birth_date"))
        .select(
            "user_sk",
            "user_id",
            "name",
            "email",
            "address",
            "city",
            "state",
            "zip",
            "birth_date",
            "created_at"
        )
        .orderBy("user_sk")
    )

    if not spark.catalog.tableExists(users_tab):
        (users_df
        .writeTo(users_tab)
        .using("iceberg")
        .partitionedBy(F.bucket(20, "user_sk"))
        .create() )
        
        return 

    # users from dwh
    df_user_dwh: SQL.DataFrame = spark.table(users_tab)

    # deleted
    df_user_del: SQL.DataFrame = df_user_dwh.join(F.broadcast(users_df), "user_sk", "left_anti")

    # final df
    # inserted, updated, unchaged + deleted
    df_user_fin: SQL.DataFrame = users_df.union(df_user_del).orderBy("user_sk")

    # save to dwh
    (
        df_user_fin
        .writeTo(users_tab)
        .using("iceberg")
        .partitionedBy(F.bucket(20, "user_sk"))
        .replace()
    )


if __name__ == "__main__":
    main()