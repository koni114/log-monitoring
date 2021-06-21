"""
- transform
  - n 개월치 data 통계값 도출하여 다시 dataFrame 으로 생성
  -
"""
from itertools import repeat, chain, cycle
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import count, avg, max, min,  stddev_samp, var_samp, round, expr
from log_detection_utils import spark_utils


class DescripStat:
    """기술 통계량을 계산해주는 클래스
    count(데이터 개수), avg(평균), max(최대값), min(최소값), std(표준편차),
    var(분산), q1(1분위 수), median(중앙값), q3(3분위 수)
    Functions
        - spark_get_quarter_stat: q1, q2, q3의 값을 spark.DataFrame 으로 return
        - spark_get_description_all_stat: count, avg, max, min, stddev_samp, var_samp return
    """
    def __init__(self, spark, df):
        self.spark = spark
        self.df = df

    def spark_get_quarter_stat(self, col_names, group_col_names=None):
        """
        1분위 수(q1), 2분위 수(q2), 3분위 수(q3)를 계산하기 위한 함수

        Args
            col_names: q1,q2,q3를 계산할 column names 입력, list
            group_col_names: 그룹별 q1,q2,q3 계산시 입력, list
        Return
            1분위수, 2분위수, 3분위수 list 값 결과 return
        """
        cols_q1 = [expr(f"percentile_approx{c}, 0.25").alias(f"q1{c}") for c in col_names]
        cols_q2 = [expr(f"percentile_approx{c}, 0.5").alias(f"q1{c}") for c in col_names]
        cols_q3 = [expr(f"percentile_approx{c}, 0.75").alias(f"q1{c}") for c in col_names]
        try:
            if group_col_names is None:
                quarter_df = self.df.agg(* cols_q1, * cols_q2, * cols_q3)
            else:
                quarter_df = self.df.groupBy(* group_col_names).agg(* cols_q1, * cols_q2, * cols_q3)
            return quarter_df
        except Exception as e:
            print("spark_get_quarter_stat functions error ! : ", e)
            raise e

    def spark_get_description_all_stat(self, col_names, group_col_names=None):
        cols_count = [count(col(c)) for c in col_names]
        cols_avg = [round(avg(col(c)), 4) for c in col_names]
        cols_max = [max(col(c)) for c in col_names]
        cols_min = [min(col(c)) for c in col_names]
        cols_std = [round(stddev_samp(col(c)), 4) for c in col_names]
        cols_var = [round(var_samp(col(c)), 4) for c in col_names]

        if group_col_names is None:
            df_stats = self.df.select(* cols_count, * cols_avg, * cols_max,
                                      * cols_min, * cols_std, * cols_var)
            df_quarter_stats = self.spark_get_quarter_stat(col_names)

            df_stats = df_stats.withColumn('id', monotonically_increasing_id())
            df_quarter_stats = df_quarter_stats.withColumn("id", monotonically_increasing_id())
            df_stats = df_stats.join(df_quarter_stats, "id", "outer").drop('id')
        else:
            df_stats = self.df.groupBy(* group_col_names).agg(* cols_count, * cols_avg, * cols_max,
                                                              * cols_min, * cols_std, * cols_var)
            df_quarter_stats = self.spark_get_quarter_stat(col_names, group_col_names)
            df_stats = df_stats.join(df_quarter_stats, group_col_names, 'inner')
        try:
            return df_stats

        except Exception as e:
            print("spark_get_description_all_stat function error! : ", e)
            raise Exception

