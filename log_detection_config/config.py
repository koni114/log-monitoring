# config.py
"""
Config class: config 정보를 가져오는 클래스
- get_db_prop:
- get_wb_prop:
"""
import os
import json


class Config:

    def __init__(self, file_path='./'):
        self.file_path = file_path
        self.file_name = None
        self.db_file_name = "db_properties.json"
        self.wb_file_name = "wb_properties.json"

    def get_prop(self):
        full_path = os.path.join(self.file_path, self.wb_file_name)
        if not os.path.isfile(full_path):
            raise FileExistsError(f"해당 경로에 파일이 없습니다. : {full_path}")
        try:
            with open(full_path) as f:
                conf_dict = json.load(f)
        except UnboundLocalError:
            print(f"해당 config file의 json format이 올바르지 않습니다: get_wb_prop --> {full_path}")
        return conf_dict

    def get_wb_prop(self):
        self.file_name = "wb_properties.json"
        conf_dict = self.get_prop()

        for key, value in conf_dict.items():
            conf_dict[key] = value.upper()

        # MODE Check
        mode = conf_dict.get("MODE", "T")
        if not mode == 'P':
            conf_dict['MODE'] = 'T'

        # WORKCODE check
        work_code = conf_dict.get("WORKCODE", "T")
        if not work_code == "K" or work_code == 'C':
            conf_dict['WORKCODE'] = 'P'

        return conf_dict

    def get_db_prop(self, db_id=None):
        if db_id is None:
            db_id = 'PPAS'
        self.file_name = "db_properties.json"
        conf_dict = self.get_prop()

        wb_prop = self.get_wb_prop()
        mode = wb_prop['MODE']
        work_code = wb_prop['WORKCODE']
        return conf_dict[mode + work_code + "_" + db_id]



