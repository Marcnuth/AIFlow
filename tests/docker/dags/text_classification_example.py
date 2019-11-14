"""
Examples for using operators
"""
from pathlib import Path
from airflow import DAG
import airflow
from datetime import timedelta
from aiflow.operators import TextClassificationDataBuildOperator
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


dag = DAG('text_classification_dataset_example', default_args=default_args, schedule_interval=None)
dag.doc_md = __doc__


logger = logging.getLogger(__name__)


seniorityDataset = TextClassificationDataBuildOperator(
    task_id='seniorityDataset',
    dag=dag,
    input_file='/resources/title_labels.csv',
    data_column='title',
    label_column='seniority',
    output_data_dir='/tmp/seniority_data/',
    output_extra_file='/tmp/seniority_data.json',
    word2vec_file='/resources/crawl-300d-2M.vec'
)

roleDataset = TextClassificationDataBuildOperator(
    task_id='roleDataset',
    dag=dag,
    input_file='/resources/title_labels.csv',
    data_column='title',
    label_column='role',
    output_data_dir='/tmp/role_data/',
    output_extra_file='/tmp/role_data.json',
    word2vec_file='/resources/crawl-300d-2M.vec'
)


seniorityDataset >> roleDataset
