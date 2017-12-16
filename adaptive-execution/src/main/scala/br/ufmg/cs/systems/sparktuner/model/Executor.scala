package br.ufmg.cs.systems.sparktuner.model

import br.ufmg.cs.systems.sparktuner.Analyzer._

import org.json4s._
import org.json4s.native.JsonMethods._

/**
 */
case class Executor(
  id: String,
  host: String,
  totalCores: Long,
  var maximumMemory: Long = 0
  ) {

  def this(jsonData: JValue) {
    this(
      getValue [String] (jsonData, "Executor ID"),
      getValue [String] (jsonData, "Executor Info", "Host"),
      getValue [Long] (jsonData, "Executor Info", "Total Cores")
      )
  }

  def addMemory(mem: Long): Unit = {
    maximumMemory += mem
  }

  override def toString = {
    s"Executor(id=${id}, host=${host}, totalCores=${totalCores})"
  }
}

object Executor {
}
