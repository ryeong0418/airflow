from airflow.operators.python import PythonOperator


def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
    import psycopg2
    from contextlib import closing
    with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, posrt=int(port))) as conn:
        with closing(conn.cursor()) as cursor:
            dag_id = kwargs.get('ti').dag_id
            task_id = kwargs.get('ti').task_id
            run_id = kwargs.get('ti').run_id
            msg = 'insrt 수행'
            sql = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
            cursor.execute(sql, (dag_id, task_id, run_id, msg))
            conn.commit()


insrt_postgres = PythonOperator(
    task_id = 'insrt_postgres',
    python_callable = insrt_postgres,
    op_args=['172.28.0.3', '5432', 'srkim', 'srkim', 'srkim']

)


def insrt_postgres(postgres_conn_id, **kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from contextlib import closing
    postgres_hook = PostgresHook(postgres_conn_id)
    with closing(postgres_hook.get_conn()) as conn:
        with closing(conn.cursor()) as cursor:
            dag_id = kwargs.get('ti').dag_id
            task_id = kwargs.get('ti').task_id
            run_id = kwargs.get('ti').run_id
            msg = 'hook insrt 수행'
            sql = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
            cursor.execute(sql, (dag_id, task_id, run_id, msg))
            conn.commit()

insrt_postgres = PythonOperator(
    task_id = 'insrt_postgres',
    python_callable=insrt_postgres,
    op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'}
)


def _calculate_stats(**context):
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date","user"]).size().reset_index()
    stats.to_csv(output_path, index=False)

    email_stats(stats, email="")

    calculate_stats >> email_stats