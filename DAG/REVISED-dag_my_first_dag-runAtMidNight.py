import datetime
import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
## ต้องแก้ไข start_date ให้เป็นวันปัจจุบันที่ run code จึงจะได้ผล
my_dag = DAG("REVISED-dag_my_first_dag-runAtMidNight", start_date=pendulum.datetime(2023, 3, 29, tz="UTC"),
             schedule_interval="@daily", catchup=False)
op = DummyOperator(task_id="task", dag=my_dag)