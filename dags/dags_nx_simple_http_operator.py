from airflow import DAG
from operators.nx_api_to_json_operator import NxApiToJsonOperator
from airflow.operators.python import PythonOperator
import pendulum
import os
import json


def save_json_locally(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='nx_info')
    file_path = 'C:/Users/seoryeong/Desktop/folder/nx_code.json'
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=4)
    print(f"JSON 데이터가 '{file_path}' 파일에 저장되었습니다.")


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

    save_to_local = PythonOperator(
        task_id='save_to_local',
        python_callable=save_json_locally,
        provide_context=True,
    )

    nx_info >> save_to_local





