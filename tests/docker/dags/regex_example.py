"""
Examples for using mongo operators
"""
from pathlib import Path
from airflow import DAG
import airflow
from datetime import timedelta
from airflow.operators import PythonOperator
from aiflow.operators.regex_operator import RegExLabellingOperator
import logging
import re
import pandas as pd


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


dag = DAG('regex_example', default_args=default_args, schedule_interval=None)
dag.doc_md = __doc__


logger = logging.getLogger(__name__)


label_seniority = RegExLabellingOperator(
    task_id='label_seniority',
    dag=dag,
    input_file='/resources/titles.csv',
    data_column='title',
    regex_rules=RegExLabellingOperator.RegExRules([
        (r'(c[a-z]o|chief|head|president|founder|co-founder)', 'Executive'),
        (r'(vice president|vp)', 'VP'),
        (r'(director)', 'Director'),
        (r'(manager)', 'Manager')
    ], 'Other', re.I),
    label_column='seniority',
    output_file='/tmp/title_seniority.csv'
)


label_role = RegExLabellingOperator(
    task_id='label_role',
    dag=dag,
    input_file='/resources/titles.csv',
    data_column='title',
    regex_rules=RegExLabellingOperator.RegExRules([
        (r'(accounting|accountant)', 'Accounting'),
        (r'(administrative)', 'Administrative'),
        (r'(arts|art|design|designer)', 'Arts and Design'),
        (r'(business development)', 'Business Development'),
        (r'(community|social services)', 'Community & Social Services'),
        (r'(consulting|consultant)', 'Consulting'),
        (r'(education)', 'Education'),
        (r'(engineer|developer|engineering)', 'Engineering'),
        (r'(entrepreneurship|entrepreneur)', 'Entrepreneurship'),
        (r'(finance)', 'Finance'),
        (r'(healthcare|medical)', 'Healthcare Services'),
        (r'(human resources|talent)', 'Human Resources'),
        (r'(information technology|it)', 'Information Technology'),
        (r'(legal)', 'Legal'),
        (r'(marketing)', "Marketing"),
        (r'(media|communications)', "Media & Communications"),
        (r'(military|protective services)', "Military & Protective Services"),
        (r'(operations|production manager)', "Operations"),
        (r'(product manager|product management)', "Product Management"),
        (r'(purchasing)', "Purchasing"),
        (r'(quality assurance|qa)', "Quality Assurance"),
        (r'(real estate)', "Real Estate"),
        (r'(research)', "Research"),
        (r'(sales|account executive)', "Sales"),
        (r'(support)', "Support")
    ], 'Other', re.I),
    label_column='role',
    output_file='/tmp/title_role.csv'
)


def combine_file(ds, **kwargs):
    df1 = pd.read_csv('/tmp/title_seniority.csv')
    df2 = pd.read_csv('/tmp/title_role.csv')

    df1['role'] = df2['role']
    df1.to_csv('/tmp/title_labels.csv', header=True, index=False)


combine = PythonOperator(
    task_id='combine',
    provide_context=True,
    python_callable=combine_file,
    dag=dag,
)

[label_seniority, label_role] >> combine
