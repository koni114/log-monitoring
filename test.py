from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, array, row_number, expr, monotonically_increasing_id
from pyspark.sql.types import *
from pyspark.sql import udf
from pyspark.sql.window import Window

from . transform_spc_tables import A3, B3, B4

spark = SparkSession.builder\
    .appName('test')\
    .master('local')\
    .getOrCreate()

test = spark.read\
    .option('header', 'true')\
    .csv('./test_data/retail-data/all/online-retail-dataset.csv')\
    .repartition(2)\
    .selectExpr("instr(Description, 'GLASS') >= 1 as is_glass")\
    .limit(10)

a = spark.createDataFrame([('Dog', "Cat"), ("Cat", "Dog"), ("Mouse", "Cat")],
                          ['Animal', 'Enemy'])

rating = [5, 4, 1]
a= spark.createDataFrame([("Dog", "Cat"), ("Cat", "Dog"), ("Mouse", "Cat")],
                              ["Animal", "Enemy"])
b = spark.createDataFrame([(l,) for l in rating], ['Rating'])
a.show()
b.show()
w = Window.orderBy(monotonically_increasing_id())
a = a.withColumn('row_idx', row_number().over(w))
b = b.withColumn('row_idx', row_number().over(w))

final_df = a.join(b, a.row_idx == b.row_idx).drop('row_idx')
final_df.show()