from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    master("local"). \
    appName("partition in spark"). \
    getOrCreate()

df = spark. \
    read. \
    option("header", True). \
    csv("C:/Users/Apurva Waghmode/PycharmProjects/pyspark-pract/sales.csv")

changedTypedf = df \
    .withColumn("Order ID", df["Order ID"].cast(IntegerType())) \
    .withColumn("Quantity Ordered", df["Quantity Ordered"].cast(IntegerType())) \
    .withColumn("Price Each", df["Price Each"].cast(FloatType()))

df2 = changedTypedf.withColumnRenamed("Order ID", "order_id"). \
    withColumnRenamed("Product", "product"). \
    withColumnRenamed("Quantity Ordered", "quantity_ordered"). \
    withColumnRenamed("Price Each", "price"). \
    withColumnRenamed("Order Date", "order_date"). \
    withColumnRenamed("Purchase Address", "address")

df2 = df2.withColumn("City", split(df2["address"], ",").getItem(1)). \
    withColumn("State", split(df2["address"], ",").getItem(2))
df2.select(concat_ws(" -",df2.City,df2.State).alias("")).show()
