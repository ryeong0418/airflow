from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd
from pprint import pprint
import logging
import requests
from airflow.models import Variable
import json
import os

class NxApiToJsonOperator(BaseOperator):

    def __init__(self, http_conn_id, endpoint, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint

    def execute(self, context):

        connection = BaseHook.get_connection(self.http_conn_id)
        logging.info(f"connection:{connection}")
        self.URI = f'{connection.host}{self.endpoint}'

        api_key = Variable.get("apikey_openapi_nx")
        headers = {'x-nxopen-api-key': api_key}

        result = requests.get(self.URI, headers=headers)
        raw_data = result.json()
        pprint(raw_data)

        file_path = 'C:/Users/seoryeong/Desktop/folder/nx_code.json'
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)
        logging.info(f"JSON 데이터가 '{file_path}' 파일에 저장되었습니다.")

        return raw_data



