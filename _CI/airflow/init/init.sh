#!/bin/bash

source .env

airflow db init

airflow users create \
  --username ${AIRFLOW_WEB_USERNAME} \
  --firstname ${AIRFLOW_WEB_FIRSTNAME} \
  --lastname ${AIRFLOW_WEB_LASTNAME} \
  --role ${AIRFLOW_WEB_ROLE} \
  --email ${AIRFLOW_WEB_EMAIL} \
  --password ${AIRFLOW_WEB_PASSWD}

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
    --conn-port ${SPARK_PORT} \
    --conn-extra '{"deploy_mode": "client"}'