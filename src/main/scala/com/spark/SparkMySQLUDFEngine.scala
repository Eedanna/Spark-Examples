package com.spark

import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.sql.SQLContext
import java.io.FileInputStream

import org.apache.spark.sql.functions.callUDF;
import org.apache.spark.sql.types.DataTypes

object SparkMySQLUDFEngine {

  def main(args: Array[String]) {

    println("<----- Starting Spark - UDF - Engine--->>>> ")

    val conf = new SparkConf().setAppName("SparkHiveConnector").setMaster("local[1]")

    val sc = new SparkContext(conf)

    // val url = "jdbc:mysql://localhost:3306/userdb"

    val prop = new java.util.Properties

    //  prop.load(new FileInputStream("file:///E:/work-space/scala/spark-examples/example/src/main/resource/dbconfig.properties"));
    prop.load(new FileInputStream("dbconfig.properties"));
    println("URL -->>> " + prop.getProperty("dbURL"))
    // prop.setProperty("url", prop.getProperty("dbURL"))
    prop.setProperty("driver", prop.getProperty("driverName"))
    prop.setProperty("user", prop.getProperty("userName"))
    prop.setProperty("password", prop.getProperty("password"))

    var url = prop.getProperty("dbURL")

    val sqlContext = new SQLContext(sc)

    val peopleDF = sqlContext.read.jdbc(url, "person", prop)

    peopleDF.show()

    sqlContext.udf.register("hashCodeInt", (s: Int, s1: Int) => (s + s1).hashCode())
    sqlContext.udf.register("hashCodeStr", (s: String, s1: String) => (s + s1).hashCode())

    // val colArray = Array("gender", "person_id")

    println("---------------------DF - INT------------------------------")
    val peopleDFInt = peopleDF.withColumn("person_hash",
      callUDF("hashCodeInt", peopleDF.col("person_id"), peopleDF.col("age")))
    peopleDFInt.show()

    println("---------------------DF - STR------------------------------")
    val peopleDFStr = peopleDF.withColumn("person_hash",
      callUDF("hashCodeStr", peopleDF.col("first_name"), peopleDF.col("gender")))
    peopleDFStr.show()

    sc.stop()
  }

}
