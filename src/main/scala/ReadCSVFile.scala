import java.security.MessageDigest

import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.google.gson.{Gson, JsonParser}
//import java.util.Base64

import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import java.util
import org.apache.commons.codec.binary.Base64


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
                // Read file as RDD
                val rdd = sqlContext.read.format("csv").option("header", "true").load("E:\\Yuvi\\Spark-Read-Excel-Sample\\MOCK_DATA.csv")
                val empDF = rdd.toDF()
                empDF.write.parquet("E:\\Yuvi\\Spark-Read-Excel-Sample\\mock_data.parquet")
                val key = "02071993"
                encryptionColumns.foreach{ fieldName =>
                        val encryptedDF = empDF.select(fieldName).rdd.map {
                                case Row(string_val: String) => (string_val, encrypt(key,string_val))
                        }.toDF(fieldName, "encrypted_"+fieldName)
                        encryptedDF.write.parquet("E:\\Yuvi\\Spark-Read-Excel-Sample\\encrypted_mock_data_"+fieldName+".parquet")
                }
                columnPartition.foreach{ i =>
                        empDF.write.partitionBy(i)parquet("E:\\Yuvi\\Spark-Read-Excel-Sample\\mock_data_partitioned_"+i+".parquet")
                }

        }


        def encrypt(key: String, value: String): String = {
                val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
                cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key))
                Base64.encodeBase64String(cipher.doFinal(value.getBytes("UTF-8")))
        }

        def decrypt(key: String, encryptedValue: String): String = {
                val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING")
                cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key))
                new String(cipher.doFinal(Base64.decodeBase64(encryptedValue)))
        }

        def keyToSpec(key: String): SecretKeySpec = {
                var keyBytes: Array[Byte] = (SALT + key).getBytes("UTF-8")
                val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
                keyBytes = sha.digest(keyBytes)
                keyBytes = util.Arrays.copyOf(keyBytes, 16)
                new SecretKeySpec(keyBytes, "AES")
        }

        private val SALT: String =
                "jMhKlOuJnM34G6NHkqo9V010GhLAqOpF0BePojHgh1HgNg8^72k"
}
