import org.apache.spark.sql.SparkSession

object CreateHiveTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Create Hive Table")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_table (
        id INT,
        name STRING,
        age INT
      )
      USING hive
    """)

    val tables = spark.sql("SHOW TABLES").collect()
    tables.foreach(table => println(s"Table: ${table.getString(1)}"))

    spark.stop()
  }
}
