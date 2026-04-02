from pyspark.sql import SparkSession

def get_spark(app_name: str = "raidar-week1") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark