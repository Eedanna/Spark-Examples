package com.spark

/**
 * @author ${user.name}
 */

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

object SparkBabyNameAgg {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkBabyNameAgg").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val input = getClass.getResource("/Baby_Names.csv").getPath()

    val babyNames = sc.textFile(input.toString())

    val babyCount = babyNames.count()

    println("babyCount ------------ >>> " + babyCount)

    val firstBabyDetails = babyNames.first()
    println("firstBabyDetails ----------->>>>> " + firstBabyDetails)

    println("-----------------------------------------------------------------------------------------------")

    val rows = babyNames.map(line => line.split(","))
    println("rows collect ----------->>>>> " + rows.collect())

    val distinctMapCount = rows.map(row => row(2)).distinct.count
    println("distinctMapCount ----------->>>>> " + distinctMapCount)

    println("-----------------------------------------------------------------------------------------------")

    val jacksonRows = rows.filter(row => row(1).contains("JACKSON"))
    println("jacksonRows ----------->>>>> " + jacksonRows)

    val jacksonRowCount = jacksonRows.count()
    println("jacksonRowCount ----------->>>>> " + jacksonRowCount)

    println("-----------------------------------------------------------------------------------------------")

    val jacksonCount = jacksonRows.filter(row => row(4).toInt > 10).count()
    println("jacksonCount ----------->>>>> " + jacksonCount)

    println("-----------------------------------------------------------------------------------------------")

    val jacksonMapCount = jacksonRows.filter(row => row(4).toInt > 10).map(row => row(2)).distinct.count
    println("jacksonMapCount ----------->>>>> " + jacksonMapCount)

    println("-----------------------------------------------------------------------------------------------")

    val names = rows.map(name => (name(1), 1))
    println("names ----------->>>>> " + names)
    names.reduceByKey((a, b) => a + b).sortBy(_._1).foreach(println _)
    println("-----------------------------------------------------------------------------------------------")

  }

}
