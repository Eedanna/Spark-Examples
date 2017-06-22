package com.spark

import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.sql.SQLContext
import java.io.FileInputStream

object SparkMySQLConnector {

  def main(args: Array[String]) {

    println("<----- Starting Spark - MySQL - Connector --->>>> ")

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

    val people = sqlContext.read.jdbc(url, "person", prop)

    people.show()

    val males = sqlContext.read.jdbc(url, "person", Array("gender='M'"), prop)
    males.show

    val first_names = people.select("first_name")
    first_names.show

    val below60 = people.filter(people("age") < 60)
    below60.show

    val grouped = people.groupBy("gender")

    val gender_count = grouped.count
    gender_count.show

    val avg_age = grouped.avg("age")
    avg_age.show

    gender_count.write.jdbc(url, "gender_count", prop)

    sc.stop()
  }

}
