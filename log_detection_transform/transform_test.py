from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from itertools import chain, repeat, cycle

spark = SparkSession.builder\
    .master('local').appName("test").getOrCreate()

manual_schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True),
])

df = spark.read.format('csv')\
    .option('header', 'true')\
    .schema(manual_schema)\
    .load("./test_data/retail-data/by-day/*.csv")
#.schema(manual_schema)\

df.printSchema()
df.show()

col_names = ['Quantity', 'UnitPrice']

test = [count(col(c)) for c in col_names]


df.select(count(col("Quantity")),  count(col("UnitPrice"))).show()
df.select(* test).show()

cols_count = [count(col(c)) for c in col_names]
cols_avg = [avg(col(c)) for c in col_names]
cols_max = [max(col(c)) for c in col_names]
cols_min = [min(col(c)) for c in col_names]
cols_quarter = [quarter(col(c)) for c in col_names]
cols_std = [stddev_samp(col(c)) for c in col_names]
cols_var = [var_samp(col(c)) for c in col_names]

