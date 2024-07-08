from airflow import DAG
from operators.nx_api_to_json_operator import NxApiToJsonOperator
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient
from airflow.decorators import task
import pendulum
import os
import json
from datetime import datetime


with DAG(
    dag_id='dags_nx_simple_http_operator',
    start_date=pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:

    nx_info = NxApiToJsonOperator(
        task_id='nx_info',
        http_conn_id='nx_api',
        endpoint='/maplestory/v1/ranking/overall?date=2024-05-01',
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='nx_info')
        print(rslt)

        blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=nxpro;AccountKey=DpkKa4WDxfHbD6TbUCyaRDYu1G838cOtDOaaLRZHQ6+a+AGjGOL6grqPpePhvJHT5SRjAvLecsC9+AStRJALvw==;EndpointSuffix=core.windows.net')
        container_client = blob_service_client.get_container_client('nxcontainer')

        raw_data = rslt
        today_date = datetime.today()
        filename = f"nx_extract_data-{today_date}.json"

        try:
            blob_client = container_client.get_blob_client(filename)
            data_json = json.dumps(raw_data, indent=4, sort_keys=True, ensure_ascii = False)
            blob_client.upload_blob(data_json, blob_type="BlockBlob")

            return "Upload successful"

        except Exception as e:
            return f"An error occurred while uploading to Blob Storage: {str(e)}"






    nx_info >> python_2()





