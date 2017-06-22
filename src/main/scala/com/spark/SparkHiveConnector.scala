package com.spark

import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.sql.SQLContext
import java.io.FileInputStream

object SparkHiveConnector {

  def main(args: Array[String]) {

    println("<----- Starting Spark - HIVE - Connector --->>>> ")

    val conf = new SparkConf().setAppName("SparkHiveConnector").setMaster("local[1]")

    val sc = new SparkContext(conf)

    val hc = new org.apache.spark.sql.hive.HiveContext(sc)

    // val df = hc.sql("FROM visapoc.emp_contact SELECT id,phno,email")

    val df = hc.sql("SELECT * FROM visapoc.emp_contact")

    df.show()

    sc.stop()
  }

}
