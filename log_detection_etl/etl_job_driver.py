"""
etl process main job 수행 module.
"""
import os
import sys
sys.path.append("/Users/heojaehun/IdeaProjects/logDetectionProject")

from pyspark.sql import SparkSession
from log_detection_utils import spark_utils
from log_detection_extract import extract_task
from log_detection_load import load_task
from pyspark.sql.functions import col

if __name__ == '__main__':

    # etl sparkSession instance
    etl_spark = spark_utils.create_spark_session(app_name='log_detection_etl',
                                                 master_type='local')

    # 0. spark option setting
    test_df = etl_spark.range(10).toDF('num')
    test_df.show(10)
    etl_spark.stop()

    # 1. splunk CSV File read
    #- data 기본 정보 파악 --> 일별 데이터가 맞는지?
    #-                 --> data Schema 정보가 일치하는지? error가 발생했다면, 다음 process 수행할 수 없음
    # extract_instance = extract_task.Extract(etl_spark)
    # log_raw_data = extract_instance.extract_from_csv_file(file_path='./test_data/retail-data/by-day/')

    #- 2. meta 정보 추출
    # log_meta_data = log_raw_data.select(col("check"))

    #- 3. meta 정보 posgreSQL에 저장
    # load_instance = load_task.Load(etl_spark)
    # load_instance.save_data_to_db(data=log_meta_data, db_id='WB')

    #- 3. save DataFrame to HDFS
    #-                   --> hadoop 저장 시에 고려해야 할 사항은 무엇인가?
    #-                       성능 향상을 위하여 파케이로 변환 후 저장
    #-                       파케이로 저장시 파티셔닝 개수를 확인해야 함 -> 파티셔닝 개수만큼 hadoop file로 저장됨.
    # load_instance.save_data_to_kudu()
    # load_instance.save_data_to_hive()

    #- 3. 최근 n개월 치 데이터 추출 from Hadoop
    #-                    --> partition 수행
    # extract_instance.extract_from_hdfs()

    #- 4. n개월치 데이터 통계값



    #- 5. n개월치 데이터를 기반, shewhart 관리도 기반 관리 규격 생성(UCL, LCL, CL)



