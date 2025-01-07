from pyspark.sql import SparkSession

def replicate():
  spark = SparkSession.builder \
      .appName("PostgresDataRead") \
      .config("spark.jars", "/opt/airflow/spark/jars/postgresql-42.2.18.jar,/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar") \
      .getOrCreate()

  # Параметры подключения к базе данных

  jdbc_url = "jdbc:postgresql://db_postgresql:5432/shop"
  properties = {
      "user": "test_user",
      "password": "test_password",
      "driver": "org.postgresql.Driver"
  }

  df = spark.read.jdbc(url=jdbc_url, table='"Users"', mode='overwrite', properties=properties)

  properties = {
      "user": "test_user",
      "password": "test_password",
      "driver": "org.mysql.Driver",
      "allowPublicKeyRetrieval": True
  }
  dest_jdbc_url = "jdbc:mysql://db_mysql:3306/shop?allowPublicKeyRetrieval=true"

  df.write.jdbc(url=dest_jdbc_url, table='Users', properties=properties)

  # Показать схему данных
  # df.printSchema()

  # # Показать первые 5 строк
  # df.show(5)

  # Закрыть SparkSession
  spark.stop()


if __name__ == "__main__":
  replicate()