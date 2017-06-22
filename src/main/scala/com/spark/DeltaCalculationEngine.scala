package com.spark

import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.sql.SQLContext
import java.io.FileInputStream

import org.apache.spark.sql.functions.callUDF;
import org.apache.spark.sql.types.DataTypes

object DeltaCalculationEngine {

  def main(args: Array[String]) {

    println("<<------- Starting Delta Calculation Engine-------- >> ")

    val conf = new SparkConf().setAppName("Spark-Delta-Calculation-Engine").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val prop = new java.util.Properties

    prop.load(new FileInputStream("dbconfig.properties"));
    prop.setProperty("driver", prop.getProperty("driverName"))
    prop.setProperty("user", prop.getProperty("userName"))
    prop.setProperty("password", prop.getProperty("password"))

    var url = prop.getProperty("dbURL")
    val sqlContext = new SQLContext(sc)
    try {

      println("-------------------------CUSTOMER_INFO--------------------------")

      val custInfoDF = sqlContext.read.jdbc(url, "userdb.customer_info", prop)
      custInfoDF.show()

      println("---------------------CUSTOMER_INFO_1----------------------------")

      val custInfo1DF = sqlContext.read.jdbc(url, "userdb.customer_info_1", prop)
      custInfo1DF.show()

      println("----------------------EXCEPT CUSTOMER_INFO_1--------------------")

      val custInfoDeltaDF = custInfoDF.except(custInfo1DF)
      custInfoDeltaDF.show

      println("----------------------EXCEPT CUSTOMER_INFO----------------------")

      val custInfo1DeltaDF = custInfo1DF.except(custInfoDF)
      custInfo1DeltaDF.show

      println("----------------------FINAL CUSTOMER INFO Delta DF--------------")

      val finalCustInfoDeltaDF = custInfoDeltaDF.unionAll(custInfo1DeltaDF)
      finalCustInfoDeltaDF.show

      println("---------Writing FINAL CUSTOMER INFO Delta Data DF to MySQL DB -------")
      finalCustInfoDeltaDF.write.jdbc(url, "userdb.customer_info_delta", prop)

    } catch {
      case ex: Exception => println("Exception occured during delta data calculation due to : " + ex.getMessage)
    } finally {
      sc.stop()
    }
  }

}
