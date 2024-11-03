from typing import Optional, Union
from datetime import datetime
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql as SQL


def create_calendar_dim(spark: SQL.SparkSession, start_dt: datetime, end_dt: datetime) -> SQL.DataFrame:
    """
    Conformed dimension that contains dates
    """

    calendar_tab = "dev.lakeh.cdim_calendar"
    
    schm_interval_dt = T.StructType([ 
        T.StructField("start_dt", T.DateType(), False), 
        T.StructField("end_dt", T.DateType(), False) ])
    
    df_interval_dt: SQL.DataFrame = spark.createDataFrame([(start_dt, end_dt)], schm_interval_dt)
    
    df_dates: SQL.DataFrame = df_interval_dt.select(
        F.explode(
            F.sequence("start_dt", "end_dt", F.expr("INTERVAL '1' DAY"))
        ).alias("date_col")
    )
    
    df_cal: SQL.DataFrame = (
        df_dates
        .withColumns({
            "date_sk": F.date_format("date_col", "yyyyMMdd").cast(T.IntegerType()),
            "date_": F.col("date_col"),
        })
        .drop("date_col")
        .withColumns({
            "year": F.expr("year(date_)").cast(T.IntegerType()),
            "quarter": F.extract(F.lit("QTR"), "date_"),
            "month": F.extract(F.lit("MON"), "date_"),
            "month_name": F.date_format("date_", "MMMM"),
            "day_of_week": F.extract(F.lit("DOW_ISO"), "date_"),
            "day_name_of_week": F.date_format("date_", "EEEE"),
            "day": F.extract(F.lit("DAY"), "date_"),
        })
    )

    (
        df_cal
        .writeTo(calendar_tab)
        .using("iceberg")
        .partitionedBy(F.years("date_"))
        .createOrReplace()
    )
    
    return df_cal


def main() -> None:
    orders_tab = "dev.lakeh.fact_orders"
    dt_col = "date_sk"

    spark: SQL.SparkSession = (
        SQL.SparkSession
        .builder
        .appName("Calendar-ETL")
        .getOrCreate()
    )

    min_dt: Optional[datetime] = None
    max_dt: Optional[datetime] = None

    if spark.catalog.tableExists(orders_tab):
        orders_df: SQL.DataFrame = spark.table(orders_tab)
        row: Optional[T.Row] = orders_df.agg(F.min(dt_col).alias("min_dt"), F.max(dt_col).alias('max_dt')).first()
        if row:
            min_dt_raw, max_dt_raw = row['min_dt'], row['max_dt']

    if min_dt_raw:
        min_dt = datetime.strptime(str(min_dt_raw), "%Y%m%d")

    if max_dt_raw:
        max_dt = datetime.strptime(str(max_dt_raw), "%Y%m%d")

    if not min_dt or not max_dt:
        min_dt = max_dt = datetime(9999, 1, 1)
        
    create_calendar_dim(spark, min_dt, max_dt)


if __name__ == "__main__":
    main()