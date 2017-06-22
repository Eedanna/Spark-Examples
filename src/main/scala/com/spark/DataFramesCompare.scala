package com.spark

import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.sql.SQLContext
import java.io.FileInputStream

import org.apache.spark.sql.functions.callUDF;
import org.apache.spark.sql.types.DataTypes

object DataFramesCompare {

  def main(args: Array[String]) {

    println("<----- Starting Spark - DataFrames compare --->>>> ")

    val conf = new SparkConf().setAppName("Spark-DataFrame-Compare").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val prop = new java.util.Properties

    prop.load(new FileInputStream("dbconfig.properties"));
    prop.setProperty("driver", prop.getProperty("driverName"))
    prop.setProperty("user", prop.getProperty("userName"))
    prop.setProperty("password", prop.getProperty("password"))

    var url = prop.getProperty("dbURL")
    val sqlContext = new SQLContext(sc)

    sqlContext.udf.register("hashCodeInt", (s: Int, s1: Int) => (s + s1).hashCode())
    // sqlContext.udf.register("hashCodeStr", (s: String, s1: String) => (s + s1).hashCode())

    // val colArray = Array("gender", "person_id")

    println("-------------------------EMP_CONTACT--------------------------")

    val empContactDF = sqlContext.read.jdbc(url, "emp_contact", prop)
    empContactDF.show()

    val empDFInt = empContactDF.withColumn("emp_hash",
      callUDF("hashCodeInt", empContactDF.col("id"), empContactDF.col("phno")))
    empDFInt.show()

    println("---------------------EMP_CONTACT_1------------------------------")

    val empContact1DF = sqlContext.read.jdbc(url, "emp_contact_1", prop)
    empContact1DF.show()

    val empDF1Int = empContact1DF.withColumn("emp1_hash",
      callUDF("hashCodeInt", empContact1DF.col("id"), empContact1DF.col("phno")))
    empDF1Int.show()

    println("---------------------------INTERSECT---------------------------------")

    val diffDF = empDFInt.intersect(empDF1Int)
    diffDF.show()

    println("----------------------EXCEPT--------------------------------------")

    val exceptDF = empDFInt.except(empDF1Int)
    exceptDF.show

    sc.stop()
  }

}
