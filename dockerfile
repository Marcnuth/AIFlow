FROM python:3.6

ENV PYTHONUNBUFFERED 1
ENV AIRFLOW_GPL_UNIDECODE yes
ENV AIRFLOW_HOME /marcnuth/airflow

RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y libev-dev apt-utils

RUN mkdir -p /marcnuth/airflow
WORKDIR /marcnuth

RUN pip install apache-airflow[celery,devel,postgres,redis,s3,ssh]
COPY requirements.txt /marcnuth/requirements.txt
RUN pip install -r /marcnuth/requirements.txt

RUN pip uninstall -y pymongo 
RUN pip uninstall -y bson 
RUN pip install  bson
RUN pip install  pymongo


ENV PYTHONPATH "${PYTHONPATH}:/marcnuth/airflow/:/marcnuth/"
ENV DEBUG_MODE 1

COPY tests/docker/dags/ /marcnuth/airflow/dags/
COPY tests/docker/config/airflow.cfg /marcnuth/airflow/
COPY tests/docker/entrypoint.sh /marcnuth/
COPY aiflow /marcnuth/aiflow

RUN airflow initdb
RUN airflow create_user -r Admin -u admin -p admin -e marcnuth@foxmail.com -f dev -l aiflow

#ENTRYPOINT ["tail", "-f","/dev/null"]
ENTRYPOINT ["sh", "/marcnuth/entrypoint.sh"]
