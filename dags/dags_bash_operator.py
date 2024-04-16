import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


with DAG(

    dag_id="dags_bash_operator", #dag 파일명과 dag_id는 일치시켜주는게 좋다.
    schedule="0 0 * * *", #분,시,일,월,요일 -> 매일마다 0시0분으로 도는 걸로 설정되어있다.
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"), #UTC는 세계 표준, Asia/Seoul는 한국 시간
    catchup=False
) as dag:
    bash_t1 = BashOperator( #Task는 오퍼레이터를 통해서 만들어지는 것
        task_id="bash_t1", #객체명과 태스크id는 일치하도록
        bash_command = "echo whoami", #bash_command는 우리가 어떤 shell script를 수행할 거냐 하는 것
    )

    bash_t2 = BashOperator(
        task_id = "bash_t2",
        bash_command = "echo $HOSTNAME"
    )

    bash_t1 >> bash_t2 #Task들의 수행순서, 관계 -> bash_t1을 수행한 후에 bash_t2를 수행한다.