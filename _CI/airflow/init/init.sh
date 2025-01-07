#!/bin/bash

source .env

airflow db init

airflow users create \
  --username admin \
  --firstname andre \
  --lastname kurepin \
  --role Admin \
  --email andrekur@yandex.ru \
  --password my_secret_pass


airflow connections add 'postgres_app' \
    --conn-type 'postgres' \
    --conn-login ${DB_POSTGRES_USER} \
    --conn-password ${DB_POSTGRES_PASSWORD} \
    --conn-host ${DB_POSTGRES_HOST} \
    --conn-port ${DB_POSTGRES_PORT} \
    --conn-schema ${DB_POSTGRES_NAME_DB}

airflow connections add 'mysql_app' \
    --conn-type 'mysql' \
    --conn-login ${DB_MYSQL_USER} \
    --conn-password ${DB_MYSQL_PASSWORD} \
    --conn-host ${DB_MYSQL_HOST} \
    --conn-port ${DB_MYSQL_PORT} \
    --conn-schema ${DB_MYSQL_NAME_DB}

airflow connections add 'spark_app' \
    --conn-type 'spark' \
    --conn-host 'spark://spark' \
    --conn-port 7077 \
    --conn-extra '{"deploy_mode": "client"}'