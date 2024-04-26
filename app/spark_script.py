import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)
from pyspark.sql import SparkSession
from decouple import config


# Create a Spark session
spark = SparkSession.builder \
    .appName("Apache iceberg practice") \
    .config("spark.jars",
            f"{project_root}/jars/postgresql-42.7.3.jar,"
            f"{project_root}/jars/iceberg-spark-runtime-3.5_2.12-1.5.1.jar") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "/Users/france.cama/code/iceberg-practice/iceberg_tables") \
    .getOrCreate()

url = f"jdbc:postgresql://localhost:5432/{config('DB_NAME')}"
properties = {
    "user": config('DB_USER'),
    "password": config('POSTGRE_PW'),
    "driver": "org.postgresql.Driver"
}
table_name = "onewayflights"

df = spark.read.jdbc(url, table_name, properties=properties)

# Write to the table
df.write.format("iceberg").mode("append").saveAsTable("onewayflights")
# Print the schema of the DataFrame
df.printSchema()
