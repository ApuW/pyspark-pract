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

csv_df = df.write. \
    option("header", True) \
    .partitionBy("continent", "region") \
    .mode("overwrite") \
    .csv("C:/Users/Apurva Waghmode/PycharmProjects/pyspark-pract/write_path")
csv_df.show()
