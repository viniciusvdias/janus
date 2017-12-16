package br.ufmg.cs.systems.sparktuner.model

import br.ufmg.cs.systems.sparktuner.Analyzer._

import org.json4s._
import org.json4s.native.JsonMethods._

/**
 */
case class Environment(
  var sparkProperties: Map[String,String] = Map.empty,
  var systemProperties: Map[String,String] = Map.empty,
  var executors: List[Executor] = Nil,
  var stages: List[Stage] = Nil
  ) {

  def this(jsonData: JValue) {
    this(
      getValue [Map[String,String]] (jsonData, "Spark Properties"),
      getValue [Map[String,String]] (jsonData, "System Properties")
    )
  }

  def addSystemProperty(key: String, value: String): Unit = {
    systemProperties += (key -> value)
  }
  
  def addSparkProperties(key: String, value: String): Unit = {
    sparkProperties += (key -> value)
  }

  def addExecutor(executor: Executor): Unit = {
    executors = executor :: executors
  }

  def addStage(stage: Stage): Unit = {
    stages = stage :: stages
  }

  override def toString = {
    s"Environment(numSparkProperties=${sparkProperties.size}, " +
      s"numSystemProperties=${systemProperties.size})"
  }
}

object Environment {
}
