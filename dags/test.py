with DAG(
    dag_id='dags_sla_email_example',
    start_date=pendulum.datetime(2023,4,1,tz='Asia/Seoul'),
    schedule='*/40 * * * *',
    catchup=False,
    defult_args={
        'sla':timedelta(minutes=5),
        'email':'ryeong2105@gmail.com'
    }
)as dag:
    task1=BashOperator(
        task_id='task1',
        bash_command='sleep 10m'
    )

    task2=BashOperator(
        task_id='task2',
        bash_command='sleep 2m'
    )