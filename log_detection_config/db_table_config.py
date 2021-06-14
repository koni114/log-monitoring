"""
db_table명, db_table의 column name 선언부
"""
from pyspark.sql.functions import col


class DBTableConfig:
    def __init__(self):
        self.db_mart = "biz_mart"
        self.job_time = ['YEAR', 'MONTH', 'DAY']
        self.spark_partition_cols = ['YEAR', 'MONTH', 'DAY']

    def get_table_cols(self, table_name):
        '''table명 입력시 컬럼 정보 return

        Args
            table_name: 테이블 명
        :return
            컬럼 명 list
        '''
        return None

    def spark_partition_cols(self):
        return [col(c) for c in self.spark_partition_cols]









