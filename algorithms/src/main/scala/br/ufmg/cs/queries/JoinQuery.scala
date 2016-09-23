package br.ufmg.cs.queries

import org.apache.spark.sql.SparkSession

class JoinQuery(rankingsPath: String, uservisitsPath: String) {

  def run: Unit = {

    val spark = SparkSession.builder().appName("JoinQuery").
      enableHiveSupport().getOrCreate()

    import spark.implicits._
    import spark.sql

    val rankingsDf = spark.read.format("parquet").load(rankingsPath)
    val uservisitsDf = spark.read.format("parquet").load(uservisitsPath)

    rankingsDf.createOrReplaceTempView ("rankings")
    uservisitsDf.createOrReplaceTempView ("uservisits")

    val joinedDf = sql (s"""SELECT sourceIP, avg(pageRank) as avgPageRank, sum(adRevenue) as totalRevenue
      FROM rankings R JOIN
        (SELECT sourceIP, destURL, adRevenue FROM uservisits UV WHERE
          (datediff(UV.visitDate, '1999-01-01')>=0 AND
            datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL)
        group by sourceIP order by totalRevenue DESC""".stripMargin.replaceAll ("\n", " "))

    joinedDf.write.save (s"rankingsUservisits-${System.currentTimeMillis}.parquet")

    spark.stop
  }
}

object JoinQuery {
  def main(args: Array[String]) {
    new JoinQuery(args(0), args(1)).run
  }
}
