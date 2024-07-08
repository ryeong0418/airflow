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

        self.URI = f'{connection.host}{self.endpoint}'

        logging.info(f"URL: {self.URI}")

        headers = {'x-nxopen-api-key': '{{var.value.apikey_openapi_nx}}'}

        result = requests.get(self.URI, headers=headers)
        raw_data = result.json()
        pprint(raw_data)

        return raw_data


