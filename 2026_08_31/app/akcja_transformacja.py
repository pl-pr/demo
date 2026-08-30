from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("app2")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

PATH = "/data/*.parquet"

df = spark.read.parquet(PATH)

df2 = df.filter("lots > 8")

df2.show()

