from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 0,
}

dag = DAG(
    'hello-turing',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def hello_turing():
    TURING = '\n'.join([
    r'  ____________       _____________',
    r' ____    |__( )_________  __/__  /________      __',
    r'____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /',
    r'___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /',
    r' _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/ v1.10.12',        
    r'  ____  ____ ',
    r' / __ \/ __ \ ',
    r'/ /_/ / / / /',
    r'\____/_/ /_/ ',
    r'  ______           _             ____  _ ',
    r' /_  ____  _______( )___  ____ _/ __ \(_)',
    r'  / / / / / / ___/ / __ \/ __ `/ /_/ / / ',
    r' / / / /_/ / /  / / / / / /_/ / ____/ /  ',
    r'/_/  \__,_/_/  /_/_/ /_/\__, /_/   /_/   ',
    r'                       /____/            ',
    ])

    print(TURING)

print_hello_turing = PythonOperator(
    task_id='print_hello_turing',
    provide_context=False,
    python_callable=hello_turing,
    dag=dag,
)

sleep = BashOperator(
        task_id="sleep", 
        bash_command="sleep 300",
        dag=dag
)

print_hello_turing >> sleep
