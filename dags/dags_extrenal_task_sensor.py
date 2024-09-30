from airflow import DAG
import pendulum
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.utils.state import State


with DAG(
    dag_id = 'dags_external_task_sensor',
    start_date=pendulum.datetime(2023,4,1,tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False
) as dag:

    external_task_sensor_a = ExternalTaskSensor(
        task_id='external_task_sensor_a',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_a',
        allowed_states=[State.SKIPPED],
        execution_delta=timedelta(hours=6),
        poke_interval=10
    )

    external_task_sensor_b = ExternalTaskSensor(
        task_id = 'external_task_sensor_b',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_b',
        failed_states=[State.SKIPPED],
        execution_delta=timedelta(hours=6),
        poke_interval=10
    )

    external_task_sensor_c = ExternalTaskSensor(
        task_id='external_task_sensor_c',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_c',
        failed_states=[State.SUCCESS],
        execution_delta=timedelta(hours=6),
        poke_interval=10
    )