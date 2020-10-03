"""
# hello-turing-nfs

An example of persisting a file to NFS using the PythonOperator

"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 0,
}

dag = DAG(
    'hello-turing-nfs-scheduled',
    default_args=default_args,
    schedule_interval='*/30 * * * *', # every 30 min
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

    epoch = int(time.mktime(time.gmtime()))
    filename = '/tmp/work/testjob/hello-turing-{}.txt'.format(epoch)
    with open(filename, 'w') as f:
        f.write(TURING)

volume_config= {
    'nfs': {
        'path': '/volume1/airflow-prod-work-dir',
        'server': '192.168.1.108' # i have an NSF running @ this address, you should change.
    }
}

print_hello_turing = PythonOperator(
    task_id='print_hello_turing',
    provide_context=False,
    python_callable=hello_turing,
    dag=dag,
    executor_config={
        'KubernetesExecutor': {
            'volumes': [{'name': 'airflow-work-dir', **volume_config}],
            'volume_mounts': [
                {
                    'mountPath': '/tmp/work/',
                    'name': 'airflow-work-dir',
                    'subPath': None,
                    'readOnly': False
                },
            ]
        }
    }    
)

print_hello_turing
