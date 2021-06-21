import builtins
import pyspark.sql.functions as F
from pyspark.sql.functions import col, monotonically_increasing_id, row_number, avg, stddev_samp
from pyspark.sql import Window
from . transform_spc_tables import A3


class ControlChart:
    def __init__(self, spark, data, group_size=10):
        self.spark = spark
        self.data = data
        self.group_size = group_size
        self.group_data = None
        self.xbar = None
        self.sbar = None
        self.lcl = None
        self.ucl = None
        self.cl = None

    def set_group_data(self, value_column):

        #- group_idx 생성
        row_num = self.data.count()
        group_num = row_num // self.group_size
        group_num_rest = row_num % self.group_size
        group_idx = [i for i in range(group_num) for _ in range(self.group_size)]
        if not group_num_rest == 0:
            group_idx.extend([group_num + 1 for _ in range(group_num_rest)])

        group_idx_df = self.spark.createDataFrame([(n, ) for n in group_idx], ['group_idx'])

        #- window 생성
        w = Window.orderBy(monotonically_increasing_id())
        group_idx_df = group_idx_df.withColumn('row_idx', row_number().over(w))
        result_df = self.data.withColumn('row_idx', row_number().over(w))

        #- 두 dataFrame 을 join 하여 group_idx 추가
        join_expression_row_idx = group_idx_df.row_idx == result_df.row_idx
        result_df = result_df.join(group_idx_df, join_expression_row_idx).drop('row_idx')

        #- ** group_idx를 기억하기 위하여 기존 data에 저장
        self.data = result_df

        #- group_idx를 기준으로 group_by_mean, std
        result_df = result_df.groupBy('group_idx').agg(
            avg(value_column).alias("X"),
            stddev_samp(value_column).alias("S")
        )

        self.group_data = result_df

    def set_xbar_and_sbar(self):
        if self.group_data is None:
            raise ValueError("get_group_data 함수를 먼저 실행해 주세요.")

        xbar_and_sbar_df = self.group_data.agg(
                                F.round(avg("X"), 4).alias("Xbar"),
                                F.round(avg("S"), 4).alias("Sbar"))

        self.xbar, self.sbar = xbar_and_sbar_df.collect()[0]

    def set_limit_line(self, table=A3):
        self.lcl = builtins.round(self.xbar - table[self.group_size] * self.sbar, 4)
        self.ucl = builtins.round(self.xbar + table[self.group_size] * self.sbar, 4)
        self.cl = self.xbar


    def get_spark_data_frame(self):
        value_list = [(self.ucl, self.cl, self.lcl, self.group_size)]
        return self.spark.createDataFrame(value_list, ['ucl', 'cl', 'lcl', 'group_size'])




