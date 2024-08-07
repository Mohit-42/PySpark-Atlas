from pyspark.sql import SparkSession

kafka_client_jar = "/home/gret/jar/kafka-clients-2.8.0.jar"
atlas_hook_jar = "/home/gret/apache-atlas-hive-hook-2.3.0/hook/hive/atlas-hive-plugin-impl/atlas-client-common-2.3.0.jar"
atlas_client_v1_jar = "/home/gret/apache-atlas-hive-hook-2.3.0/hook/hive/atlas-hive-plugin-impl/atlas-client-v1-2.3.0.jar"
atlas_client_v2_jar = "/home/gret/apache-atlas-hive-hook-2.3.0/hook/hive/atlas-hive-plugin-impl/atlas-client-v2-2.3.0.jar"
atlas_common_jar = "/home/gret/apache-atlas-hive-hook-2.3.0/hook/hive/atlas-hive-plugin-impl/atlas-common-2.3.0.jar"
atlas_intg_jar = "/home/gret/apache-atlas-hive-hook-2.3.0/hook/hive/atlas-hive-plugin-impl/atlas-intg-2.3.0.jar"
atlas_notification_jar = "/home/gret/apache-atlas-hive-hook-2.3.0/hook/hive/atlas-hive-plugin-impl/atlas-notification-2.3.0.jar"

# Other required jars
spark_atlas_connector_jar = "/home/gret/jar/spark-atlas-connector_2.12-3.3.2.7.2.17.0-334.jar"
commons_configuration_jar = "/home/gret/jar/commons-configuration-1.10.jar"

all_jars = ",".join([
    kafka_client_jar,
    atlas_hook_jar,
    atlas_client_v1_jar,
    atlas_client_v2_jar,
    atlas_common_jar,
    atlas_intg_jar,
    atlas_notification_jar,
    spark_atlas_connector_jar,
    commons_configuration_jar
])

# Initialize SparkSession with Hive support and Atlas configurations
spark = SparkSession.builder \
    .appName("Create Hive Table") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.extraListeners", "com.hortonworks.spark.atlas.SparkAtlasEventTracker") \
    .config("spark.sql.queryExecutionListeners", "com.hortonworks.spark.atlas.SparkAtlasEventTracker") \
    .config("spark.jars", all_jars) \
    .config("spark.driver.extraClassPath", all_jars) \
    .config("spark.executor.extraClassPath", all_jars) \
    .config("spark.atlas.rest.address", "http://localhost:21000") \
    .config("spark.atlas.auth.type", "basic") \
    .config("spark.atlas.username", "admin") \
    .config("spark.atlas.password", "admin") \
    .getOrCreate()

# Define the table schema and create the Hive table
spark.sql("""
    CREATE TABLE t3_n_spark AS
    SELECT id, name
    FROM test_table_n
""")

# Optionally, you can check if the values were inserted successfully
df = spark.sql("SELECT * FROM t3_n_spark")
df.show()

# Stop the SparkSession
spark.stop()
