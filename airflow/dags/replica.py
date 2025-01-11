from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

with DAG(
    'replicate_tables',
    default_args=DEFAULT_ARGS,
    description='Replica from PostgreSQL to MySQL',
    schedule_interval=timedelta(days=1),
) as dag:
  tables = ('Users', 'ProductCategories', 'Products', 'Orders', 'OrderDetails', )

  start = EmptyOperator(task_id='start')
  finish = EmptyOperator(task_id='finish')

  for table in tables:
      # Параметры подключения
      spark_submit_task = SparkSubmitOperator(
          task_id=f'replicate_{table}',
          application='/opt/airflow/scripts/replicate_table.py',
          conn_id='spark_app',
          application_args=[
            table
          ],
          conf={
              "spark.driver.memory": "600m",
              "spark.executor.memory": "600m"
          },
          jars='/opt/airflow/spark/jars/postgresql-42.2.18.jar,/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar'
      )

      start >> spark_submit_task >> finish