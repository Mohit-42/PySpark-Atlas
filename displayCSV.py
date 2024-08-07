from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Display CSV").getOrCreate()

# Load the CSV file from HDFS
df = spark.read.csv("/user/samplesales/sales_data_sample.csv", header=True, inferSchema=True)

# Show the contents of the CSV file
df.show()

# Stop the Spark session
spark.stop()
