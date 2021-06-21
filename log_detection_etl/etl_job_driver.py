#!/usr/bin/env python3
"""
etl process main job 수행 module.
"""
import os
import sys
import importlib
import re

sys.path.append("/Users/heojaehun/IdeaProjects/logDetectionProject")


from log_detection_utils import spark_utils
from log_detection_extract import extract_task
from log_detection_load import load_task
from log_detection_transform import transform_stats, transform_spc, transform_spc_tables
from log_detection_dm import dm_manager
from pyspark.sql.functions import col, lit, current_date, stddev_samp

# importlib.reload(transform_spc)


if __name__ == '__main__':
    # etl sparkSession instance
    etl_spark = spark_utils.create_spark_session(app_name='log_detection_etl',
                                                 master_type='local')


    # 1. splunk CSV File read
    #- data 기본 정보 파악 --> 일별 데이터가 맞는지?
    #-                 --> data Schema 정보가 일치하는지? error가 발생했다면, 다음 process 수행할 수 없음
    extract_instance = extract_task.Extract(etl_spark)
    log_raw_data = extract_instance.extract_from_csv_file(file_path='./test_data/retail-data/by-day/',
                                                          file_name='*.csv',
                                                          header='true',
                                                          inferSchema='true',
                                                          ignoredLeadingWhiteSpace='true',
                                                          ignoreTrailingWhiteSpace='true',
                                                          numPartitions=10
                                                          )


    #- 2. 컬럼명 --> 영문으로 변경
    kor_eng_col_names_dict = {key:value for key, value
                              in zip(log_raw_data.columns, ['aggr_dt', 'chain_id', 'app_id', 'page_name', 'page_type',
                                                            'total_time', 'loading_time', 'rendering_time', 'user_id',
                                                            'user_ip', 'browser_type'])}

    log_raw_data = log_raw_data.select([col(c).alias(kor_eng_col_names_dict.get(c, 'NaN'))
                                        for c in log_raw_data.columns])

    #- 3. 데이터 전처리
    #- 3.1 결측이 존재하는 데이터는 drop --> 데이터 개수에 비해 결측은 크게 많지 않기 때문
    log_raw_data = log_raw_data.na.drop('any')

    #- 4. meta 정보 posgreSQL에 저장
    #- 4.1 tb_log_app_info select chain_id, app_id, page_type 호출
    log_app_info_save_sql = """(SELECT chain_id, app_id, page_name, page_type, FROM, tb_log_app_info) AS distinct_app_info"""
    tb_log_app_info = extract_instance.extract_from_db(sql=log_app_info_save_sql, db_id='PPAS')
    log_meta_data = log_raw_data.select('chain_id', 'app_id', 'page_name', 'page_type').distinct()

    #- 4.2 join expression 제작
    #- 현재 DB에 없는 Meta 정보만 저장하기 위하여 left anti 사용
    join_type = 'left_anti'
    join_expression_chain_id = log_meta_data['chain_id'] == tb_log_app_info['chain_id']
    join_expression_app_id = log_meta_data['app_id'] == tb_log_app_info['app_id']
    join_expression_page_type = log_meta_data['page_type'] == tb_log_app_info['page_type']

    #- 4.3 log_meta_data, tb_log_app_info left anti join
    log_new_meta_data = log_meta_data.join(tb_log_app_info, (join_expression_chain_id &
                                                             join_expression_app_id &
                                                             join_expression_page_type), join_type)

    #- 4.4 anti join 결과를 다시 tb_log_app_info에 저장
    load_instance = load_task.Load(etl_spark)
    if log_new_meta_data.count() > 0:
        log_app_info_instance = dm_manager.LogAppInfo(log_new_meta_data)
        log_app_info_data = log_app_info_instance.get_data()
        load_instance.save_data_to_db(data=log_app_info_data,
                                      target_table='tb_log_app_info',
                                      db_id='PPAS',
                                      save_mode='append')


    #- 5. save DataFrame to HDFS
    #- 성능 향상을 위한 파케이 변환 parquet
    #- parquet로 저장시, partitioning 개수 확인

    #- 6. n개월 치 데이터 추출 from hadoop
    n_months_row_data = log_raw_data

    #- 7. n개월 치 데이터 통계값 계산
    value_col = ['loading_time']
    group_col = ['chain_id', 'app_id', 'page_name', 'page_type']

    stats_instance = transform_stats.DescripStat(spark=etl_spark, df=n_months_row_data)
    tmp_stats_df = stats_instance.spark_get_description_all_stat(group_col_names=group_col,
                                                                 col_names=value_col)


    #- 컬럼명 변경
    regex_string = "count|avg|max|min|std|var|q1|q2|q3"
    stats_col_mapping_dict = {"count": "cnt", "avg": "mean_v", "max": "max_v",
                              "min": "min_v", "std": "sd_v", "var": "var_v", "q1": "q1_v",
                              "q2": "medi_v", "q3": "q3_v"}

    regex_compiler = re.compile(regex_string)
    stat_col_renames = [stats_col_mapping_dict.get(regex_compiler.findall(c)[0], "")
                        for c in tmp_stats_df.columns]
    stats_df = tmp_stats_df.toDF(* group_col, * stat_col_renames)

    #- 통계값(stats_df) DB 저장
    log_stat_instance = dm_manager.LogStat(stats_df)
    log_stat_data = log_stat_instance.get_data()
    load_instance.save_data_to_db(data=log_stat_data,
                                  target_table='tb_log_stat',
                                  db_id='PPAS',
                                  save_mode='append')

    #- 5. Shewhart 관리도 기반 관리 규격 생성(UCL, CL, LCL)
    #- 5.1 해석용 관리도를 위한 통계값 계산
    log_meta_data_collect = log_meta_data.collect()
    qcc_stat_df = None
    for (idx, (chain_id, app_id, page_name, page_type)) in log_meta_data_collect:

        chain_id_filter = col('chain_id') == lit(chain_id)
        app_id_filter = col('app_id') == lit(app_id)
        page_name_filter = col('page_name') == lit(page_name)
        page_type_filter = col('page_type') == lit(page_type)

        n_months_row_data_slicing_by_meta = n_months_row_data.filter(chain_id_filter &
                                                                     app_id_filter &
                                                                     page_name_filter &
                                                                     page_type_filter)

        count_value = n_months_row_data_slicing_by_meta.count()
        if count_value >= 100:
            group_size = 5

            #- 해석용 관리도 통계값 생성
            spc_instance = transform_spc.ControlChart(spark=etl_spark,
                                                      data=n_months_row_data_slicing_by_meta,
                                                      group_size=group_size)

            spc_instance.set_group_data(value_column=value_col)
            spc_instance.set_xbar_and_sbar()
            spc_instance.set_limit_line()

            qcc_stat_tmp_df = spc_instance.get_spark_data_frame()
            qcc_stat_D_tmp_df = qcc_stat_tmp_df.withColumn("chain_id", lit(chain_id)) \
                                               .withColumn("app_id", lit(app_id)) \
                                               .withColumn("qcc_tp", lit('D')) \
                                               .withColumn("use_tp", lit('X')) \
                                               .withColumn("page_name", lit(page_name)) \
                                               .withColumn("page_type", lit(page_type))

            if qcc_stat_df is None:
                qcc_stat_df = qcc_stat_D_tmp_df
            else:
                qcc_stat_df = qcc_stat_df.unionAll(qcc_stat_D_tmp_df).coalesce(1)

            #- 관리용 관리도 통계값 생성
            group_data = spc_instance.group_data
            under_ucl_filter = group_data.X < lit(spc_instance.ucl)
            over_lcl_filter = group_data.X > lit(spc_instance.lcl)
            remove_anomaly_group_data = group_data.where(under_ucl_filter & over_lcl_filter)

            spc_instance.group_data = remove_anomaly_group_data
            spc_instance.set_xbar_and_sbar()
            spc_instance.set_limit_line()

            print(f"xbar: {spc_instance.xbar}, sbar: {spc_instance.sbar}, group_size: {spc_instance.group_size}",
                  f" lcl: {spc_instance.lcl}, cl: {spc_instance.cl}, ucl: {spc_instance.ucl}")


            qcc_stat_tmp_df = spc_instance.get_spark_data_frame()
            qcc_stat_M_tmp_df = qcc_stat_tmp_df.withColumn("chain_id", lit(chain_id)) \
                .withColumn("app_id", lit(app_id)) \
                .withColumn("qcc_tp", lit('M')) \
                .withColumn("use_tp", lit('X')) \
                .withColumn("page_name", lit(page_name)) \
                .withColumn("page_type", lit(page_type))

            qcc_stat_df = qcc_stat_df.unionAll(qcc_stat_M_tmp_df).coalesce(1)


    #- 관리도 통계값 저장
    log_qcc_stat_instance = dm_manager.LogQccStat(qcc_stat_df)
    log_qcc_stat_data = log_qcc_stat_instance.get_data()
    load_instance.save_data_to_db(data=log_qcc_stat_data,
                                  target_table='tb_log_qcc_stat',
                                  db_id='PPAS',
                                  save_mode='append')


























