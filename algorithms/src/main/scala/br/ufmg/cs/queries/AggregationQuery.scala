package br.ufmg.cs.queries

import org.apache.spark.sql.SparkSession

class AggregationQuery(uservisitsPath: String) {

  def run: Unit = {

    val spark = SparkSession.builder().appName("AggregationQuery").
      enableHiveSupport().getOrCreate()

    import spark.implicits._
    import spark.sql

    val uservisitsDf = spark.read.format("parquet").load(uservisitsPath)

    uservisitsDf.createOrReplaceTempView ("uservisits")

    val aggregatedDf = sql ("SELECT sourceIP, SUM(adRevenue) as sumAdRevenue FROM uservisits GROUP BY sourceIP")

    aggregatedDf.write.save (s"aggregatedUservisits-${System.currentTimeMillis}.parquet")

    spark.stop
  }
}

object AggregationQuery {
  def main(args: Array[String]) {
    new AggregationQuery(args(0)).run
  }
}
