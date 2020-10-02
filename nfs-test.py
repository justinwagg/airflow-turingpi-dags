from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': datetime.utcnow(),
    "retries": 0,
}

dag = DAG("nfs-test", default_args=default_args, schedule_interval=timedelta(1), catchup=False)


t1 = BashOperator(
        task_id="t1", 
        bash_command="echo 'hello world' > /var/airflow-work-dir/airflow-test.txt", 
        dag=dag
    )
