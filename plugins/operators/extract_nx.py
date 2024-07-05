import requests
#from azure.storage.blob import BlobServiceClient
from pprint import pprint
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
import json
from dotenv import load_dotenv


class ExtractNxOperator(BaseOperator):

    def __init__(self, api_key, uri, file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)  # BaseOperator의 초기화 호출
        self.API_KEY = api_key
        self.URI = uri
        self.FILE_PATH = file_path

    def execute(self, context):

        # 디버그 출력 추가
        print(f"API_KEY: {self.API_KEY}")
        print(f"URI: {self.URI}")
        print(f"FILE_PATH: {self.FILE_PATH}")

        if not self.URI:
            raise ValueError("URI가 설정되지 않았습니다.")

        headers = {'x-nxopen-api-key': self.API_KEY}
        result = requests.get(self.URI, headers=headers)
        raw_data = result.json()
        pprint(raw_data)

        with open(self.FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)

        print("JSON 데이터가 'raw_data.json' 파일에 저장되었습니다.")

        return raw_data


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
