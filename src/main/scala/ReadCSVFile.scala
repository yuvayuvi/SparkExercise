import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object ReadCSVFile {
        case class Employee(id:String, first_name:String, last_name:String, email:String, gender:String, ip_address:String , card_no:String, mobile_no: String, ssn: String)
        def main(args : Array[String]): Unit = {
                var conf = new SparkConf().setAppName("Read_CSV_File").setMaster("local[*]")
                val sc = new SparkContext(conf)
                val sqlContext = new SQLContext(sc)
                import sqlContext.implicits._
                /*val textRDD = sc.textFile("E:\\Yuvi\\Spark-Read-Excel-Sample\\MOCK_DATA.csv")
                //println(textRDD.foreach(println)
                val empRdd = textRDD.map {
                        line =>
                                val col = line.split(",")
                                Employee(col(0), col(1), col(2), col(3), col(4), col(5), col(6), col(7), col(8))
                }
                val empDF = empRdd.toDF()
                empDF.write.parquet("E:\\Yuvi\\Spark-Read-Excel-Sample\\mock_data.parquet")*/
//                empDF.show()

                val spark: SparkSession = SparkSession.builder()
                        .master("local[1]")
                        .appName("Yuvi_Test")
                        .getOrCreate()
                val parqDF = spark.read.parquet("E:\\Yuvi\\Spark-Read-Excel-Sample\\mock_data.parquet\\part-00000-a5c12a66-a1c2-4b9a-a630-cf1c3113f770-c000.snappy.parquet")
                parqDF.createOrReplaceTempView("ParquetTable")
                spark.sql("select * from ParquetTable where id != 'id'").explain()
                val parkSQL = spark.sql("select * from ParquetTable where id != 'id'")
                parkSQL.show()
                parkSQL.printSchema()
        }
}
