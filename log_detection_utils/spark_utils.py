"""
util function
- create_spark_session: spark application 수행을 위한 sparkSession 객체 생성
- 현재 날짜를 원하는 포멧에 맞게 계산
- input Dataframe 기본 정보 파악하여 print 해주는 함수
"""
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from functools import reduce


def create_spark_session(app_name, master_type):
    spark = SparkSession\
        .builder\
        .master(master_type)\
        .appName(app_name)\
        .getOrCreate()

    return spark


def union_all(*dfs):
    return reduce(DataFrame.union, dfs)
