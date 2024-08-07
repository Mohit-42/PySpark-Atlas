from pyspark.sql import SparkSession

# Initialize SparkSession with Hive support
spark = SparkSession.builder \
    .appName("Create Hive Table") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .getOrCreate()

# Define the table schema and create the Hive table
spark.sql("""
    CREATE TABLE t3_spark AS
    SELECT id, name
    FROM test_table
""")

# Optionally, you can check if the values were inserted successfully
df = spark.sql("SELECT * FROM t3_spark")
df.show()

# Stop the SparkSession
spark.stop()
