import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Apache iceberg practice") \
    .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={project_root}/iceberg_tables") \
    .config("spark.jars",
            f"{project_root}/jars/iceberg-spark-runtime-3.5_2.12-1.5.1.jar") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", f"{project_root}/iceberg_tables") \
    .getOrCreate()

df = spark.read.csv(f"{project_root}/titanic.csv", header=True, inferSchema=True)

# Write to the iceberg table
df.write.format("iceberg").mode("append").saveAsTable("titanic")

# Print the schema of the DataFrame
df.printSchema()

