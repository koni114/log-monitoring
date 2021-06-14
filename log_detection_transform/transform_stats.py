"""
- transform
  - n 개월치 data 통계값 도출하여 다시 dataFrame 으로 생성
  -
"""
from itertools import repeat, chain, cycle
from pyspark.sql.functions import col
from pyspark.sql.functions import count, avg, max, min,  stddev_samp, var_samp
from log_detection_utils import spark_utils


class DescripStat:
    """기술 통계량을 계산해주는 클래스
    Functions
        - spark_get_quarter_stat: q1, q2, q3의 값을 spark.DataFrame으로 return
        - spark_get_description_all_stat: count, avg, max, min, stddev_samp, var_samp return
    """
    def __init__(self, spark, df):
        self.spark = spark
        self.df = df

    def spark_get_quarter_stat(self, col_names):
        """
        Args
            col_names:

        Return
            1분위수, 2분위수, 3분위수 list 값 결과 return
        """
        try:
            quarter_value_list = [self.df.approxQuantile(c, [0.25, 0.5, 0.75], 0) for c in col_names]
            flat_quarter_value = [value for row in quarter_value_list for value in row]
            col_repeat_names = list(chain.from_iterable([repeat(col_name, 3)
                                                         for col_name in col_names]))
            quarter_it = cycle(['q1', 'q2', 'q3'])
            quarter_marks = [next(quarter_it) for _ in range(len(col_names) * 3)]
            quarter_col_names = [quarter_mark + "(" + col_name + ")"
                                 for col_name, quarter_mark in zip(col_repeat_names, quarter_marks)]
            return self.spark.createDataFrame([flat_quarter_value], quarter_col_names)
        except Exception as e:
            print("spark_get_quarter_stat function error ! :", e)
            raise Exception


    def spark_get_description_all_stat(self, col_names):
        cols_count = [count(col(c)) for c in col_names]
        cols_avg = [avg(col(c)) for c in col_names]
        cols_max = [max(col(c)) for c in col_names]
        cols_min = [min(col(c)) for c in col_names]
        cols_std = [stddev_samp(col(c)) for c in col_names]
        cols_var = [var_samp(col(c)) for c in col_names]

        try:
            df_stats = self.df.select(* cols_count, * cols_avg, * cols_max,
                                      * cols_min, * cols_std, * cols_var)

            df_quarter_stats = self.spark_get_quarter_stat(col_names)
            df_stats = spark_utils.union_all(df_stats, df_quarter_stats)
            return df_stats

        except Exception as e:
            print("spark_get_description_all_stat function error! : ", e)
            raise Exception

