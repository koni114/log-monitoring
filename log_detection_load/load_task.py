"""
- Loading
  - load csv file to HDFS
  - load statistics to postgreSQL
  - load control chart UCL, LCL, CL value
"""
from log_detection_config import config


class Load:
    def __init__(self, spark):
        self.spark = spark
        self.config = config.Config()
        self.wb_config = None

    def set_config_info(self):
        self.wb_config = self.config.get_wb_prop()

    def save_data_to_db(self, data, target_table, db_id='WB'):
        result = "SUCCESS"
        db_config = self.config.get_db_prop(db_id)
        try:
            data.write.mode("append")\
                .jdbc(db_config.get("URL"), target_table, db_config)
        except Exception as e:
            result = "ERROR"
        return result

    def save_data_to_kudu(self, data, target_table, db_id='HDFS'):
        result = "SUCCESS"
        db_config = self.config.get_db_prop(db_id)
        try:
            data.write.mode("append")\
                .jdbc(db_config.get("URL"), target_table, db_config)
        except Exception as e:
            result = "ERROR"
        return result

    def save_data_to_hive(self, data, target_table, db_id="HDFS"):
        result = "SUCCESS"
        db_config = self.config.get_db_prop(db_id)
        data.createOrReplaceTempView(target_table)
        try:
            data.write.mode("append")\
                .insertInto(target_table)
            self.spark.sql(f"refresh {target_table}")
        except Exception as e:
            print("Error: save hive")
            result = "ERROR"
        return result





