import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


spark = (
    SparkSession.builder
    .appName("app1")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

PATH = "/data/*.parquet"

print()
print("TEST 1: PySpark")

start = time.time()

df = spark.read.parquet(PATH)

result = (
    df
    .withColumn(
        "lots2",
        col("lots") * 2
    )
    .selectExpr(
        "sum(lots2)"
    )
    .collect()[0][0]
)

elapsed = time.time() - start

print("Result:", result)

print(f"Time: {elapsed:.2f} seconds")


print()
print("TEST 2: Python UDF")


@udf(IntegerType())
def double_lots(lots):

    return lots * 2


start = time.time()

df = spark.read.parquet(PATH)

result = (
    df
    .withColumn(
        "lots2",
        double_lots(col("lots"))
    )
    .selectExpr(
        "sum(lots2)"
    )
    .collect()[0][0]
)

elapsed = time.time() - start

print("Result:", result)

print(f"Time: {elapsed:.2f} seconds")

print()
input("pause...")


spark.stop()