from pyspark.sql import SparkSession

# Set the absolute paths to the Iceberg tables and JAR files
iceberg_tables_path = "/Users/france.cama/code/iceberg-practice/iceberg_tables"
iceberg_jars_path = "/Users/france.cama/code/iceberg-practice/jars/iceberg-spark-runtime-3.5_2.12-1.5.1.jar"

# Create a Spark session
spark = SparkSession.builder \
    .appName("Iceberg time travel feature") \
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + iceberg_tables_path) \
    .config("spark.jars", iceberg_jars_path) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", iceberg_tables_path) \
    .getOrCreate()


current_catalog = spark.catalog.currentCatalog()
print(f"Current Catalog: {current_catalog}")
# show snapshots details
spark.sql("SELECT * FROM default.titanic.history ORDER BY made_current_at DESC;").show()

# time travel to timestamp
df = spark.sql(f"SELECT * FROM default.titanic TIMESTAMP AS OF '2024-07-22T11:10:00.289+00:00';")
df.show(5)

# time travel using snapshot_id
df_id = spark.sql(f"SELECT * FROM default.titanic VERSION AS OF 4619981039604578990;")
df_id.show(5)


### ROLL BACK ###
# Roll back an Iceberg table to a previous snapshot using either a timestamp or a snapshot ID.
# Users who have been assigned the ADMIN role, the table's owner, and users with INSERT, UPDATE, or DELETE privileges on the table can use the ROLLBACK command.
# ROLLBACK TABLE default.titanic TO { SNAPSHOT '6609739537041086161'};
SNAPSHOT = 6609739537041086161
spark.sql(f"CALL spark_catalog.system.rollback_to_snapshot('default.titanic', {SNAPSHOT})")

print("TITANIC ROLLED BACK")
spark.sql("SELECT * FROM titanic;").show(5)
