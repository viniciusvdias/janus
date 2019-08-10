package br.ufmg.cs.systems.sparktuner.rdd

import br.ufmg.cs.systems.common.Logging

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object InterceptorTestApp {
  def main(args: Array[String]) {
    // spark
    val conf = new SparkConf().
      setAppName("Simple Application").
      setMaster("local")
    val sc = new SparkContext(conf)
    println (s"Application name: ${sc.appName}")
    sc.setLogLevel ("ERROR")
    val numbers = sc.parallelize (1 to 10000, 100)
    println (s"numbers partitions = ${numbers.getNumPartitions}")
    println (s"numbers count = ${numbers.count}")
    sc.stop()
  }
}
