package com.spark

import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.sql.SQLContext
import java.io.FileInputStream

import org.apache.spark.sql.functions.callUDF;
import org.apache.spark.sql.types.DataTypes

object RDDTransformations {

  def main(args: Array[String]) {

    println("<----- Starting Spark - RDD Transformations--->>>> ")

    val conf = new SparkConf().setAppName("SparkHiveConnector").setMaster("local[1]")

    val sc = new SparkContext(conf)

    try {

      val input = getClass.getResource("/Baby_Names.csv").getPath()

      val babyNames = sc.textFile(input.toString())

      val babyCount = babyNames.count()

      println("babyCount --->> " + babyCount)

      println("----------------------MAP(FUNC)--------------------------------------")

      val rows = babyNames.map(line => line.split(","))
      rows foreach println

      println("----------------------FLATMAP(FUNC)--------------------------------------")

      val rowsInFlat = babyNames.flatMap(line => line.split(","))
      rowsInFlat foreach println

      println("----------------------FILTER(FUNC)--------------------------------------")

      val filteredRows = babyNames.filter(line => line.contains("JANSON"))
      filteredRows foreach println

      println("----------------------groupByKey()--------------------------------------")
      val namesToCounties = rows.map(name => (name(1), name(2)))
      val groupByCountries = namesToCounties.groupByKey.collect
      groupByCountries foreach println
      
      println("----------------------reduceByKey()--------------------------------------")
      

    } catch {
      case ex: Exception => println("Exception occured due to " + ex.getMessage)
    } finally {
      sc.stop()
    }
  }

}
