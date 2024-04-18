from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "dags_bah_select_fruit",
    schedulr = "10 0 * * 6#1", #매월 첫 번째 주 토요일 0시10분
    start_date = pendulum.datetime(2023,3,1,tz="Asia/Seoul"),
    catchup = False
) as dag:

    t1_orange = BashOperator(
        task_id = "t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruits.sh ORANGE",
    )

    t2_avocado = BashOperator(
        task_id = "t2_avocado",
        bash_command="/opt/airflow/plugins/shell/select_fruits.sh AVOCADO",
    )


    t1_orange >> t2_avocado
