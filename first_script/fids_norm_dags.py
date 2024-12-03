# Конфигурация DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='norm_data_processing',
    default_args=default_args,
    description='Processing and inserting data into norm_flats',
    schedule_interval='@hourly',  # Запускается каждый час
    start_date=datetime(2023, 11, 1),
    catchup=False,
) as dag:

    process_data = PythonOperator(
        task_id='process_and_insert_data',
        python_callable=process_and_insert_data,
    )

    process_data
