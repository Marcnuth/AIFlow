FROM python:3.6

ENV PYTHONUNBUFFERED 1
ENV AIRFLOW_GPL_UNIDECODE yes
ENV AIRFLOW_HOME /marcnuth/airflow

RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y libev-dev apt-utils
RUN apt-get install -y locales

ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US
ENV LC_ALL en_US.UTF-8  
ENV AIRFLOW_HOME /marcnuth/airflow

RUN mkdir -p /marcnuth/airflow
RUN mkdir -p /marcnuth/aiflow
WORKDIR /marcnuth

RUN pip install apache-airflow[celery,devel,postgres,redis,s3,ssh]
COPY requirements.txt /marcnuth/requirements.txt
RUN pip install -r /marcnuth/requirements.txt

RUN pip uninstall -y pymongo 
RUN pip uninstall -y bson 
RUN pip install  bson
RUN pip install  pymongo
RUN python -m nltk.downloader wordnet stopwords punkt

ENV PYTHONPATH "${PYTHONPATH}:/marcnuth/airflow/:/marcnuth/"
ENV DEBUG_MODE 1
ENV C_FORCE_ROOT 1
ENV AIRFLOW_CONN_ELASTIC_LOCAL "elasticsearch:9200"

COPY aiflow /marcnuth/aiflow
COPY tests/docker/dags/ /marcnuth/airflow/dags/
COPY tests/docker/config/airflow.cfg /marcnuth/airflow/
COPY tests/docker/entrypoint.sh /marcnuth/

RUN airflow initdb
RUN airflow create_user -r Admin -u admin -p admin -e marcnuth@foxmail.com -f dev -l aiflow

#ENTRYPOINT ["tail", "-f","/dev/null"]
ENTRYPOINT ["sh", "/marcnuth/entrypoint.sh"]
