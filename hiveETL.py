from pyspark.sql import SparkSession, Row

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

# Initialize SparkSession with Atlas configurations
spark = SparkSession.builder \
    .appName("Sales Analysis with Atlas Integration") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.extraListeners", "com.hortonworks.spark.atlas.SparkAtlasEventTracker") \
    .config("spark.sql.queryExecutionListeners", "com.hortonworks.spark.atlas.SparkAtlasEventTracker") \
    .config("spark.sql.streaming.streamingQueryListeners","com.hortonworks.spark.atlas.SparkAtlasStreamingQueryEventTracker")\
    .config("spark.jars", all_jars) \
    .config("spark.driver.extraClassPath", all_jars) \
    .config("spark.executor.extraClassPath", all_jars) \
    .config("spark.atlas.rest.address", "http://localhost:21000") \
    .config("spark.atlas.auth.type", "basic") \
    .config("spark.atlas.username", "admin") \
    .config("spark.atlas.password", "admin") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.sql.atlas.hook.enabled", "true") \
    .getOrCreate()

# Define the schema
RecSales = Row('ordernumber', 'quantityordered', 'priceeach', 'orderlinenumber', 'sales',
               'orderdate', 'status', 'qtr_id', 'month_id', 'year_id', 'productline', 'msrp', 'productcode', 'customername',
               'phone', 'addressline1', 'addressline2', 'city', 'state', 'postalcode', 'country', 'territory', 'contactlastname', 'contactfirstname', 'dealsize')

# Load the data
dataSales = spark.sparkContext.textFile("/user/samplesales/")
header = dataSales.first()
dataSales = dataSales.filter(lambda line: line != header)
recSales = dataSales.map(lambda l: l.split(","))
dataSales = recSales.map(lambda l: RecSales(l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11], l[12], l[13], l[14], l[15], l[16], l[17], l[18], l[19], l[20], l[21], l[22], l[23], l[24]))
dfRecSales = spark.createDataFrame(dataSales)
dfRecSales.write.mode("overwrite").saveAsTable("sales")  

# Simple queries
spark.sql("SELECT count(*) FROM sales").show()  # 
spark.sql("SELECT * FROM sales").show()  
spark.sql("SELECT ordernumber, priceeach FROM sales").show(2)  

# Group by and write results ato Hive table
dfterriroty = spark.sql("SELECT territory, sum(priceeach) AS total FROM sales GROUP BY territory")  #
dfterriroty.createOrReplaceTempView("sumterr_spark")
dfterriroty.show()

# Create Hive table and insert results
spark.sql("""CREATE TABLE IF NOT EXISTS territory_sum_spark AS 
          SELECT territory, total 
          FROM sumterr_spark""")

# Verify data written to Hive
spark.sql("SELECT * FROM territory_sum_spark").show()
spark.sql("SELECT count(*) FROM territory_sum_spark").show()

spark.stop()
