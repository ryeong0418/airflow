from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd
from pprint import pprint
import logging
import requests
import json


class NxApiToJsonOperator(BaseOperator):

    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'nx_api'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_nx}}/maplestory/v1/ranking/overall?date=2024-05-01'
        self.base_dt = base_dt

    def execute(self, context):

        # Logging for debugging
        logging.info(f"conn_id: {self.http_conn_id}")
        logging.info(f"path: {self.path}")
        logging.info(f"file_name: {self.file_name}")
        logging.info(f"endpoint: {self.endpoint}")

        if not self.URI:
            raise ValueError("URI가 설정되지 않았습니다.")

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'{connection.host}:{connection.port}/{self.endpoint}'
        headers = {'x-nxopen-api-key': '{{var.value.aapikey_openapi_nx}}'}
        print(headers)
        # result = requests.get(self.URI, headers=headers)
        # raw_data = result.json()
        # pprint(raw_data)

        # with open(self.FILE_PATH, 'w', encoding='utf-8') as f:
        #     json.dump(raw_data, f, ensure_ascii=False, indent=4)
        #
        # return raw_data
