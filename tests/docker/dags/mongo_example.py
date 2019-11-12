"""
Examples for using mongo operators
"""
from pathlib import Path
from airflow import DAG
import airflow
from datetime import timedelta
from aiflow.operators import MongoToCSVOperator
import logging


default_args = {
    'owner': 'aiflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['aiflow@aiflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('mongo_example', default_args=default_args, schedule_interval=None)
dag.doc_md = __doc__


logger = logging.getLogger(__name__)


export2file = MongoToCSVOperator(
    task_id='export2file',
    dag=dag,
    mongo_conn_id='mongo_default',
    mongo_collection='lead',
    mongo_database='mg_prod',
    mongo_query={"title": {"$exists": True}},
    output_fields=['title', 'contactInfo'],
    output_file='/tmp/export.csv',
    limit=10,
    op_kwargs=dict()
)


export2file
