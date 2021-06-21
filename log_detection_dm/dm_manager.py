from pyspark.sql.functions import col, current_date, lit


class DBTableInfo:
    def __init__(self, data):
        self._data = None
        self.data = data
        self._column_name = data.columns

    def get_column_name_info(self) -> list:
        raise NotImplementedError

    def get_table_name_info(self) -> str:
        raise NotImplementedError

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        column_list = self.get_column_name_info()
        for name in data.columns:
            if name not in column_list:
                data = data.drop(name)
        self._data = data


    @property
    def column_name(self):
        return self._column_name

    @column_name.setter
    def column_name(self, column_names):
        table_column_name = self.get_column_name_info()
        for name in column_names:
            if name not in self.get_column_name_info():
                raise ValueError(f"'{name}' not in {table_column_name!r}")
        self._column_name = column_names

    def get_data(self):
        return_df = self._data
        return_df_columns = return_df.columns

        table_column_name = self.get_column_name_info()
        for column_name in table_column_name:
            if column_name not in return_df_columns:
                raise ValueError(f"'{column_name}' is not in {table_column_name!r}")

        return return_df.select([col(c) for c in table_column_name])


class LogAppInfo(DBTableInfo):
    def __init__(self, data):
        super().__init__(data)

    def get_column_name_info(self) -> list:
        return ['chain_id', 'app_id', 'page_name', 'page_type']


    def get_table_name_info(self) -> str:
        return "tb_log_app_info"


class LogStat(DBTableInfo):
    def __init__(self, data):
        super().__init__(data)

    def get_column_name_info(self) -> list:
        return ['aggr_dt', 'chain_id', 'app_id', 'stat_tp',
                'fact', 'cnt', 'mean_v', 'sd_v', 'var_v', 'min_v',
                'max_v', 'medi_v', 'q1_v', 'q3_v']

    def get_table_name_info(self) -> str:
        return "tb_log_stat"

    def get_data(self):
        log_stat_columns = self._data.columns
        return_df = self._data
        if 'aggr_dt' not in log_stat_columns:
            return_df = return_df.withColumn('aggr_dt', current_date())
        if 'stat_tp' not in log_stat_columns:
            return_df = return_df.withColumn('stat_tp', lit('M'))
        if 'fact' not in log_stat_columns:
            return_df = return_df.withColumn('fact', lit('A'))

        return_df_columns = return_df.columns
        log_stat_data_column_info = self.get_column_name_info()
        for column_name in log_stat_data_column_info:
            if column_name not in return_df_columns:
                raise ValueError(f"'{column_name}' is not in {log_stat_data_column_info!r}")

        return return_df.select([col(c) for c in log_stat_data_column_info])


class LogQccStat(DBTableInfo):
    def __init__(self, data):
        super().__init__(data)

    def get_column_name_info(self) -> list:
        return ['aggr_dt', 'chain_id', 'app_id', 'qcc_tp',
                'use_tp', 'ucl', 'cl', 'lcl', 'group_size']

    def get_table_name_info(self) -> str:
        return "tb_log_qcc_stat"


    def get_data(self):
        log_stat_columns = self._data.columns
        return_df = self._data
        if 'aggr_dt' not in log_stat_columns:
            return_df = return_df.withColumn('aggr_dt', current_date())
        if 'qcc_tp' not in log_stat_columns:
            return_df = return_df.withColumn('qcc_tp', lit('D'))
        if 'use_tp' not in log_stat_columns:
            return_df = return_df.withColumn('use_tp', lit('X'))

        return_df_columns = return_df.columns
        qcc_stat_data_column_info = self.get_column_name_info()
        for column_name in qcc_stat_data_column_info:
            if column_name not in return_df_columns:
                raise ValueError(f"'{column_name}' is not in {qcc_stat_data_column_info!r}")

        return return_df.select([col(c) for c in qcc_stat_data_column_info])


class LogQccPlot(LogQccStat):
    def __init__(self, data):
        super().__init__(data)

    def get_column_name_info(self) -> list:
        return ['aggr_dt', 'chain_id', 'app_id', 'use_tp',
                'plot_nm', 'plot_file_nm', 'plot_content']

    def get_table_name_info(self) -> str:
        return "tb_log_qcc_plot"
