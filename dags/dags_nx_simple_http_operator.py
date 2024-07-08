from airflow import DAG
from operators.nx_api_to_json_operator import NxApiToJsonOperator
import pendulum

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





