from airflow import DAG
from operators.nx_api_to_json_operator import NxApiToJsonOperator
from azure.storage.blob import BlobServiceClient
from airflow.decorators import task
import pendulum
import json
from datetime import datetime
from airflow.models import Variable


with DAG(
    dag_id='dags_nx_simple_http_operator',
    start_date=pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    catchup=False,
    schedule_interval='* * * * *'

) as dag:

    start_date = pendulum.datetime(2024, 5, 1, tz='Asia/Seoul')
    end_date = pendulum.datetime(2024, 6, 1, tz='Asia/Seoul')

    date = start_date

    while date < end_date:
        current_date_str = date.format('YYYY-MM-DD')

    nx_info = NxApiToJsonOperator(
        task_id=f'nx_info_{current_date_str}',
        http_conn_id='nx_api',
        endpoint=f'/maplestory/v1/ranking/overall?date={current_date_str}',
    )

    @task(task_id=f'python_{current_date_str}')
    def python_task(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids=f'nx_info_{current_date_str}')

        conn_str = Variable.get('connection_string')
        container_name = Variable.get('container_name')
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service_client.get_container_client(container_name)

        raw_data = rslt
        filename = f"nx_extract_data-{current_date_str}.json"

        try:
            blob_client = container_client.get_blob_client(filename)
            data_json = json.dumps(raw_data, indent=4, sort_keys=True, ensure_ascii = False)
            blob_client.upload_blob(data_json, blob_type="BlockBlob")

            return "Upload successful"

        except Exception as e:
            return f"An error occurred while uploading to Blob Storage: {str(e)}"


    nx_info >> python_task(current_date_str)

    date = date.add(days=1)





