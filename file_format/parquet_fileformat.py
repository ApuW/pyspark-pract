from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    master("local"). \
    appName("partition in spark"). \
    getOrCreate()

df = spark. \
    read. \
    option("header", True). \
    csv("C:/Users/Apurva Waghmode/PycharmProjects/pyspark-pract/country.csv")

parquet_df = df.write. \
    option("header", True) \
    .partitionBy("continent", "region") \
    .mode("overwrite") \
    .parquet("C:/Users/Apurva Waghmode/PycharmProjects/pyspark-pract/write_path")

