package com.spark

import org.apache.spark.{ SparkConf, SparkContext }

object BasicMap {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-BasicMap").setMaster("local")

    val sc = new SparkContext(conf)
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println("-------->>>"+result.collect().array.length) // Array : 4
    println("---->>>"+result.collect().mkString) // 14916    
    println(result.collect().mkString(",")) // 1,4,9,16
  }
}