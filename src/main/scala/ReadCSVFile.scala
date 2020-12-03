import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.google.gson.{Gson, JsonParser}
import java.util.Base64


object ReadCSVFile {
        case class ParseOptions(ingerSchema : String, delimiter : String, header : String)
        case class Input(master : String, appName : String, source : String,  target : String, writeMode : String, columnPartition : Array[String], encryptionColumns : Array[String], parseOptions : ParseOptions)
        case class Employee(id:String, first_name:String, last_name:String, email:String, gender:String, ip_address:String , card_no:String, mobile_no: String, ssn: String)
        def main(args : Array[String]): Unit = {
                val gson = new Gson
                val parser = new JsonParser
                val ip = parser.parse("{\"master\":\"spark://Ashoks-MacBook-Pro.local:7077\",\"appName\":\"sample3\",\"source\":\"E:\\Yuvi\\Spark-Read-Excel-Sample\\MOCK_DATA.csv\",\"target\":\"E:\\Yuvi\\Spark-Read-Excel-Sample\\mock_data.parquet\",\"writeMode\":\"OVERWRITE\",\"columnPartition\":[\"first_name\", \"gender\"],\"encryptionColumns\":[\"card_no\"],\"parseOptions\":{\"inferSchema\":\"true\",\"delimiter\":\",\",\"header\":\"true\"}}").getAsJsonObject
                val jsonObject = gson.fromJson(ip, classOf[Input])
                val source = jsonObject.source
                val target = jsonObject.target
                val delimiter = jsonObject.parseOptions.delimiter
                val columnPartition = jsonObject.columnPartition
                val encryptionColumns = jsonObject.encryptionColumns

                //To convert CSV to Parquet file
                val conf = new SparkConf().setAppName("Read_CSV_File").setMaster("local[*]")
                val sc = new SparkContext(conf)
                val sqlContext = new SQLContext(sc)
                import sqlContext.implicits._
                val textRDD = sc.textFile("E:\\Yuvi\\Spark-Read-Excel-Sample\\MOCK_DATA.csv")
                val empRdd = textRDD.map {
                        line =>
                                val col = line.split(delimiter)
                                val id = if(encryptionColumns.contains("id")) convertBase64(col(0)) else col(0)
                                val firstName = if(encryptionColumns.contains("first_name")) convertBase64(col(1)) else col(1)
                                val lastName = if(encryptionColumns.contains("last_name")) convertBase64(col(2)) else col(2)
                                val email = if(encryptionColumns.contains("email")) convertBase64(col(3)) else col(3)
                                val gender = if(encryptionColumns.contains("gender")) convertBase64(col(4)) else col(4)
                                val ipAddress = if(encryptionColumns.contains("ip_address")) convertBase64(col(5)) else col(5)
                                val cardNo = if(encryptionColumns.contains("card_no")) convertBase64(col(6)) else col(6)
                                val mobileNo = if(encryptionColumns.contains("mobile_no")) convertBase64(col(7)) else col(7)
                                val ssn =  if(encryptionColumns.contains("ssn")) convertBase64(col(8)) else col(8)

                                Employee(id, firstName, lastName, email, gender, ipAddress, cardNo, mobileNo, ssn)
                }
                val empDF = empRdd.toDF()
                empDF.write.parquet("E:\\Yuvi\\Spark-Read-Excel-Sample\\mock_data.parquet")
                columnPartition.foreach{ i =>
                        empDF.write.partitionBy(i)parquet("E:\\Yuvi\\Spark-Read-Excel-Sample\\mock_data_partitioned_"+i+".parquet")
                }

                //To read Parquet file
                /*val spark: SparkSession = SparkSession.builder()
                        .master("local[1]")
                        .appName("Yuvi_Test")
                        .getOrCreate()
                val parqDF = spark.read.parquet("E:\\Yuvi\\Spark-Read-Excel-Sample\\mock_data.parquet\\part-00000-d03247d2-65cf-4fb1-a9c3-b8b795ff1544-c000.snappy.parquet")
                parqDF.createOrReplaceTempView("ParquetTable")
                spark.sql("select * from ParquetTable where id != 'id'").explain()
                val parkSQL = spark.sql("select * from ParquetTable where id != 'id'")
                parkSQL.show()
                parkSQL.printSchema()*/
        }

        def convertBase64(ip : String): String = {
                val encoder = Base64.getEncoder
                encoder.encodeToString(ip.getBytes)
        }

        def decodeBase64(base64 : String): String ={
                val decoder = Base64.getUrlDecoder
                val bytes = decoder.decode(base64)
                new String(bytes)
        }
}
