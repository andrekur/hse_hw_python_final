from datetime import timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

from defaults import DEFAULT_ARGS, DEFAULT_SPARK_SUBMIT_CONF, JARS


with DAG(
	'calc_dm_product_stats',
	default_args=DEFAULT_ARGS,
	description='Оverwrite data mart by product stats',
	schedule_interval=timedelta(days=1),
) as dag:

	start = EmptyOperator(task_id='start')
	finish = EmptyOperator(task_id='finish')

	spark_submit_task = SparkSubmitOperator(
		task_id=f'calc_dm_product_stats',
		application='/opt/airflow/scripts/calc_dm_product_stats.py',
		conn_id='spark_app',
		conf=DEFAULT_SPARK_SUBMIT_CONF,
		jars=JARS
	)

	start >> spark_submit_task >> finish
