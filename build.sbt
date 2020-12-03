name := "Spark-Read-Excel-Sample"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies++=Seq(
        "org.apache.spark" %% "spark-core" % "2.4.0",
        "org.apache.spark" %% "spark-sql" % "2.4.0",
        "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0",
        "com.google.code.gson" % "gson" % "1.7.1"
)