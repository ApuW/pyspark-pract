from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession. \
    builder. \
    master("local"). \
    appName("partition in spark"). \
    getOrCreate()

df = spark. \
    read. \
    option("header", True). \
    csv("C:/Users/Apurva Waghmode/PycharmProjects/pyspark-pract/sales.csv")

df.show(5)
df.printSchema()

changedTypedf = df \
    .withColumn("Order ID", df["Order ID"].cast(IntegerType())) \
    .withColumn("Quantity Ordered", df["Quantity Ordered"].cast(IntegerType())) \
    .withColumn("Price Each", df["Price Each"].cast(FloatType())) \
    .withColumn("Order Date", df["Order Date"].cast(DateType()))

changedTypedf.printSchema()

