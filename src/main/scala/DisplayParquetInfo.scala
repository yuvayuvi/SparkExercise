import org.apache.spark.sql.SparkSession

object DisplayParquetInfo {
        def main(args : Array[String]): Unit = {
                //To read Parquet file
                val spark: SparkSession = SparkSession.builder()
                        .master("local[1]")
                        .appName("Yuvi_Test")
                        .getOrCreate()
                val parqDF = spark.read.parquet("E:\\Yuvi\\Spark-Read-Excel-Sample\\mock_data.parquet\\part-00000-b6e4bf40-21ea-477c-befd-dc76ee393b1f-c000.snappy.parquet")
                parqDF.createOrReplaceTempView("ParquetTable")
                spark.sql("select * from ParquetTable").explain()
                val parkSQL = spark.sql("select * from ParquetTable")
                parkSQL.show()
                parkSQL.printSchema()
        }

}
