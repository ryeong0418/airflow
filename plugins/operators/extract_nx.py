import requests
#from azure.storage.blob import BlobServiceClient
from pprint import pprint
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
import json
from dotenv import load_dotenv
import logging


class ExtractNxOperator(BaseOperator):

    def __init__(self, api_key, uri, file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)  # BaseOperator의 초기화 호출
        self.API_KEY = api_key
        self.URI = uri
        self.FILE_PATH = file_path

    def execute(self, context):

        # Logging for debugging
        logging.info(f"API_KEY: {self.API_KEY}")
        logging.info(f"URI: {self.URI}")
        logging.info(f"FILE_PATH: {self.FILE_PATH}")

        if not self.URI:
            raise ValueError("URI가 설정되지 않았습니다.")

        headers = {'x-nxopen-api-key': self.API_KEY}
        try:
            response = requests.get(self.URI, headers=headers)
            response.raise_for_status()
            raw_data = response.json()
            pprint(raw_data)

            # Save the data to a JSON file
            with open(self.FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(raw_data, f, ensure_ascii=False, indent=4)

            logging.info(f"JSON data has been saved to {self.FILE_PATH}.")
            return raw_data

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            raise


    # def load_data(self):
    #
    #     print("load_data")
    #
    #     blob_service_client = BlobServiceClient.from_connection_string(self.CONNECTION_STRING)
    #     container_client = blob_service_client.get_container_client(self.CONTAINER_NAME)
    #
    #     raw_data = self.extract_data()
    #     today_date = datetime.today()
    #     filename = f"nx_extract_data-{today_date}.json"
    #
    #     try:
    #         blob_client = container_client.get_blob_client(filename)
    #         data_json = json.dumps(raw_data, indent=4, sort_keys=True, ensure_ascii = False)
    #         blob_client.upload_blob(data_json, blob_type="BlockBlob")
    #
    #         return "Upload successful"
    #
    #     except Exception as e:
    #         return f"An error occurred while uploading to Blob Storage: {str(e)}"
