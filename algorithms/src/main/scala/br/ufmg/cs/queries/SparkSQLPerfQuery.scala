package br.ufmg.cs.queries

import br.ufmg.cs.systems.common.Logging

import org.apache.spark.sql.SparkSession

import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.tpcds.Tables
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SparkSQLPerfQueries(scaleFactor: Int,
    tpcdsLocation: String,
    queryName: String,
    iterations: Int = 3,
    tablesFormat: String = "parquet") extends Logging {

  def run: Unit = {

    val spark = SparkSession.builder().appName("SparkSQLPerfQueries").
      enableHiveSupport().getOrCreate()

    import spark.implicits._
    import spark.sql

    val tables = new Tables(spark.sqlContext, "", scaleFactor)
    val tpcds = new TPCDS (sqlContext = spark.sqlContext)
    tables.createTemporaryTables(tpcdsLocation, tablesFormat)
    val experiment = tpcds.runExperiment(
      Seq(queryName).map(tpcds.queriesMap),
      iterations = iterations)

    experiment.waitForFinish (Int.MaxValue)

    experiment.currentResults.foreach { result =>
      logInfo (s"${result.name} ${result.tables.mkString("[", ",", "]")} ${result.executionTime} ms")
    }

    spark.stop
  }
}

object SparkSQLPerfQueries {
  def main(args: Array[String]) {
    new SparkSQLPerfQueries(args(0).toInt, args(1), args(2), args(3).toInt).run
  }
}
