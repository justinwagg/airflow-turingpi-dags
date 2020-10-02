from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
# from airflow.kubernetes.volume import Volume
# from airflow.kubernetes.volume_mount import VolumeMount
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 0,
}

dag = DAG(
    'hello-turing-nfs',
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

    time.sleep(300)

    with open('/tmp/work/test.txt') as f:
        f.write(TURING)


# volume_mount = VolumeMount(
#     'nfs-disk',
#     mount_path='/tmp/work',
#     sub_path=None,
#     read_only=False
# )

# volume_config= {
#     'persistentVolumeClaim':
#     {
#         'claimName': 'airflow-work-pvc'
#     }
# }

# volume = Volume(name='airflow-work-pv', configs=volume_config)

print_hello_turing = PythonOperator(
    task_id='print_hello_turing',
    provide_context=False,
    python_callable=hello_turing,
    dag=dag,
    executor_config={
        "KubernetesExecutor": {
            "volumes": [
                {
                    "name": "airflow-work-dir",
                    "hostPath": {"path": "/volume1/airflow-prod-work-dir"},
                },
            ],
            "volume_mounts": [
                {
                    "mountPath": "/tmp/work/",
                    "name": "airflow-work-dir",
                },
            ]
        }
    }    
)

print_hello_turing
