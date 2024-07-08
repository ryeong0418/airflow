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
        logging.info(f"connection:{connection}")

        self.URI = f'{connection.host}/{self.endpoint}'

        logging.info(f"URL: {self.URI}")

        headers = {'x-nxopen-api-key': 'test_a0adc891b0b450f50ae02074b90875c34c5316ea505452cb5e7e5ba6af6eb93fefe8d04e6d233bd35cf2fabdeb93fb0d'}

        result = requests.get(self.URI, headers=headers)
        raw_data = result.json()
        pprint(raw_data)

        return raw_data


