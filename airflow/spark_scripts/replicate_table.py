import sys

from pyspark.sql import SparkSession
from dotenv import dotenv_values

CONFIG = dotenv_values('.env')

class ConnectionConfig:
  def __init__(self, user, db_type, host, port, db_name, table_name, schema=None) -> None:
    db_drivers = {
      'postgresql': 'org.postgresql.Driver',
      'mysql': 'com.mysql.jdbc.Driver'
    }

    self.user = user
    self.driver = db_drivers[db_type]
    self.conn_url = f'jdbc:{db_type}://{host}:{port}/{db_name}'
    self.table = f'{schema}.{table_name}' if schema else table_name


# TODO вынести в общую функцию репликации
def replicate(from_db, to_db):
  spark = SparkSession.builder \
      .appName("PostgresDataRead") \
      .config("spark.jars", "/opt/airflow/spark/jars/postgresql-42.2.18.jar,/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar") \
      .getOrCreate()

  df = spark.read \
    .format("jdbc") \
    .option("url", from_db.conn_url) \
    .option("dbtable", from_db.table) \
    .option("user", from_db.user['login']) \
    .option("password", from_db.user['passwd']) \
    .option("driver", from_db.driver) \
    .load()

  df.write \
    .format("jdbc") \
    .option("url", to_db.conn_url) \
    .option("dbtable", to_db.table) \
    .option("user", to_db.user['login']) \
    .option("password", to_db.user['passwd']) \
    .option("driver", to_db.driver) \
    .mode("overwrite") \
    .save()

  spark.stop()


if __name__ == "__main__":
  replicate_table = sys.argv[1] # table name get from args

  postgres_config = ConnectionConfig(
    {'login': CONFIG['DB_POSTGRES_USER'], 'passwd': CONFIG['DB_POSTGRES_PASSWORD']},
    'postgresql',
    CONFIG['DB_POSTGRES_HOST'],
    CONFIG['DB_POSTGRES_PORT'],
    CONFIG['DB_POSTGRES_NAME_DB'],
    f'"{replicate_table}"',
    'public'
  )

  mysql_config = ConnectionConfig(
    {'login': CONFIG['DB_MYSQL_USER'], 'passwd': CONFIG['DB_MYSQL_PASSWORD']},
    'mysql',
    CONFIG['DB_MYSQL_HOST'],
    CONFIG['DB_MYSQL_PORT'],
    CONFIG['DB_MYSQL_NAME_DB'],
    replicate_table,
  )

  replicate(postgres_config, mysql_config)