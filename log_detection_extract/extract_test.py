from pyspark.sql import SparkSession
from log_detection_extract import extract_task
from log_detection_config import config
import os


spark = SparkSession.builder\
    .master('local').appName("test")\
    .config('spark.driver.extraClassPath', './lib/postgresql-9.4.1207.jar')\
    .config('spark.executor.cores', '4')\
    .getOrCreate()


extract_test = extract_task.Extract(spark)
test_df = extract_test.extract_from_csv_file(file_path='./test_data/retail-data/by-day/',
                                             header='true', inferSchema='true')
test_df.rdd.getNumPartitions()
test_df = extract_test.extract_from_db(db_id='PPAS', sql='dual')
test_df.count()
