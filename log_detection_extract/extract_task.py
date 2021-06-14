"""
- Extraction
  - HDFS File Extraction(binding)
  - postgreSQL Extraction
  - read the csv file
"""
from log_detection_config import config
import os


class Extract:
    def __init__(self, spark):
        self.spark = spark
        self.config = config.Config()
        self.wb_config = None

    def set_config_info(self):
        self.wb_config = self.config.get_wb_prop()

    def extract_from_hdfs(self, file_name, schema):
        result_data = self.spark.read.schema(schema) \
            .option("header", "true") \
            .option("mode", "DROPMALFORMED") \
            .option("delimiter", ",") \
            .option("encoding", "UTF-8") \
            .option("charset", "UTF-8") \
            .csv("hdfs:///" + file_name)
        return result_data

    def extract_from_db(self, sql, db_id=None):
        db_config = self.config.get_db_prop(db_id)
        result_data = self.spark.read.format("jdbc")\
            .option("encoding", "UTF-8")\
            .option("driver", "")\
            .option("url", db_config.get("URL"))\
            .option("dbtable", sql)\
            .option("user", db_config.get("USER"))\
            .option("password", db_config.get("PWD"))

        return result_data

    def extract_from_csv_file(self, file_name=None, file_path='./', **kwargs):

        if not os.path.isdir(file_path):
            raise ValueError(f"file path 가 정확하지 않습니다. : {file_path}")

        if file_name is None:
            full_path_name = os.path.join(file_path, "*.csv")
        else:
            full_path_name = os.path.join(file_path, file_name)
            if not os.path.isfile(full_path_name):
                raise FileExistsError(f"파일이 존재하지 않습니다! : {full_path_name}")

        try:
            result_data = self.spark.read.format("csv")\
                .options(** kwargs).load(full_path_name)
        except Exception as e:
            raise ValueError(f"option 값을 확인하세요. : {kwargs!r}")

        return result_data









