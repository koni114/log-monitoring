from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName('test')\
    .master('local')\
    .config('spark.driver.bindAddress', '127.0.0.1')\
    .getOrCreate()

