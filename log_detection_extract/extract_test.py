from pyspark.sql import SparkSession
import os


spark = SparkSession.builder\
    .master('local').appName("test")\
    .getOrCreate()

extract = Extract(spark)
test_df = extract.extract_by_csv_file(file_path="./test_data/retail-data/by-day", header="true")
test_df.count()
