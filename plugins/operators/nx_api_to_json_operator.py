from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd
from pprint import pprint
import logging
import requests
import json


class NxApiToJsonOperator(BaseOperator):

    def __init__(self, http_conn_id, endpoint, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint

    def execute(self, context):

        # Logging for debugging
        logging.info(f"conn_id: {self.http_conn_id}")
        logging.info(f"endpoint: {self.endpoint}")

        connection = BaseHook.get_connection(self.http_conn_id)
        self.URL = f'{connection.host}/{self.endpoint}'

        logging.info(f"endpoint: {self.URL}")

        headers = {'accept':'application/json',
                   'x-nxopen-api-key': '{{var.value.aapikey_openapi_nx}}'}

        print(headers)

        result = requests.get(self.URI, headers=headers)
        raw_data = result.json()
        pprint(raw_data)

        return raw_data


