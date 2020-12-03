import org.apache.spark.sql.SparkSession

object DisplayParquetInfo {
        def main(args : Array[String]): Unit = {
                //To read Parquet file
                val spark: SparkSession = SparkSession.builder()
                        .master("local[1]")
                        .appName("Yuvi_Test")
                        .getOrCreate()
                val parqDF = spark.read.parquet("E:\\Yuvi\\Spark-Read-Excel-Sample\\mock_data.parquet\\part-00000-635e9d78-8eff-4268-9404-f4a731602e9a-c000.snappy.parquet")
                parqDF.createOrReplaceTempView("ParquetTable")
                spark.sql("select * from ParquetTable where id != 'id'").explain()
                val parkSQL = spark.sql("select * from ParquetTable where id != 'id'")
                parkSQL.show()
                parkSQL.printSchema()
        }

}
