import pyspark

spark = pyspark.sql.SparkSession.builder \
    .appName("LocalTest") \
    .master("local[*]") \
    .getOrCreate()

print(spark.sparkContext)
