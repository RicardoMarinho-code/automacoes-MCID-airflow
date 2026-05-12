import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

host = os.environ.get('host')
port = os.environ.get('port')
database = os.environ.get('database')
user = os.environ.get('user')
password = os.environ.get('password')

default_args = {
   'owner': 'Aluizio Cidral Júnior',
   'depends_on_past': False,
   'start_date': days_ago(2),
   'retries': 1,
   }

with DAG(
   'DAG-indicium',
   schedule_interval='0 */2 * * *',
   default_args=default_args
   ) as dag:

   t1 = BashOperator(
   task_id='task1',
   bash_command="""
   cd $AIRFLOW_HOME/dags/tasks/
   python3 task1.py {{ execution_date }}
   """)
   
   t2 = BashOperator(
   task_id='task2',
   bash_command="""
   cd $AIRFLOW_HOME/dags/tasks/
   python3 task2.py {{ execution_date }}
   """)

   t3 = BashOperator(
   task_id='task3',
   bash_command="""
   cd $AIRFLOW_HOME/dags/tasks/
   python3 task3.py {{ execution_date }}
   """)

   t4 = BashOperator(
   task_id='task_email',
   bash_command="""
   cd $AIRFLOW_HOME/dags/tasks/
   python3 task_email.py {{ execution_date }}
   """)

[t1,t2] >> t3 >> t4