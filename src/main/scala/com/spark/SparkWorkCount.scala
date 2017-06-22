package com.spark

import java.io._
import org.apache.spark.{ SparkConf, SparkContext }
import scala.io.Source

object SparkWorkCount {

  def main(args: Array[String]) {

    //  println(args(0) + "<<<<<----------------------->>>>> " + args(1))
    /*val input = Source.fromURL(getClass.getResource(args(0)))
   		val output = Source.fromURL(getClass.getResource(args(1)))
		*/

    /* val input = Source.fromFile(args(0))
       val output = Source.fromFile(args(1))*/

    val input = getClass.getResource("/wordcount.txt").getPath()

    println(input + "<<<<<----------------------->>>>> ")

    val conf = new SparkConf().setAppName("Spark-WordCount").setMaster("local")

    val sc = new SparkContext(conf)

    val file = sc.textFile(input.toString(), 3)

    println("File -->>> " + file)

    val words = file.flatMap(line => line.split(" "))

    println("words -->>> " + words)

    val wordCount = words.map(word => (word, 1)).reduceByKey((x, y) => x + y)

    println("wordCount -->>> " + wordCount)

    wordCount.saveAsTextFile("wordcount_output.txt")

    println("Result saved to output file -->>> ")

  }
}