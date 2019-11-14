airflow initdb
airflow create_user -r Admin -u admin -p admin -e marcnuth@foxmail.com -f dev -l aiflow


# airflow worker -D & # this need only when using CeleryExecutor
airflow scheduler -D &
airflow webserver 
