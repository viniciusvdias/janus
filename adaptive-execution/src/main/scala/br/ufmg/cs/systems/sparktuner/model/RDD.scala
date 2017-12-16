package br.ufmg.cs.systems.sparktuner.model

import br.ufmg.cs.systems.common.Logging

import br.ufmg.cs.systems.sparktuner.Analyzer._

import org.json4s._
import org.json4s.native.JsonMethods._

/**
 * Instances of this class hold information regarding RDDs identified in the
 * execution log. Also can represent an 'adaptation point', which is an
 * opportunity to change the application's parallelism.
 */
case class RDD(
    val stageId: Long,
    val id: Long,               // identifies the RDD
    val _name: String,          // identifies the adaptive point
    val scope: Long,            // some operators generate more than one RDD
    val operator: String,       // operator's name
    val parents: Array[Long],   // ids of parent RDDs
    val callSite: String,        // contains the place in the code that this RDD was created
    val cachedPartitions: Long,
    val useMem: Boolean,
    val useDisk: Boolean,
    val useExternal: Boolean) {

  RDD.add (this)

  private var _stage: Stage = _

  def stage: Stage = _stage

  def setStage(s: Stage): Unit = {
    _stage = s
  }

  def this(stageId: Long, jsonData: JValue) {
    this(
      stageId,
      getValue [Long] (jsonData, "RDD ID"),
      getValue [String] (jsonData, "Name"),
      getValue [String] (jsonData, "Scope").replaceAll ("\"|}|", "").
        replaceAll(",",":").split(":")(1).toLong,
      getValue [String] (jsonData, "Scope").replaceAll ("\"|}", "").split(":").last,
      getValue [Seq[BigInt]] (jsonData, "Parent IDs").map(pid => pid.toLong).toArray,
      getValue [String] (jsonData, "Callsite"),
      getValue [Long] (jsonData, "Number of Cached Partitions"),
      getValue [Boolean] (jsonData, "Storage Level", "Use Memory"),
      getValue [Boolean] (jsonData, "Storage Level", "Use Disk"),
      getValue [Boolean] (jsonData, "Storage Level", "Use ExternalBlockStore")
      )
  }

  def name: String = {
    _name
    //singleName
    //if (_name contains "adaptive") {
    //  _name
    //} else if (parents.length == 0) {
    //  singleName
    //} else {
    //  s"${parents.map(p => RDD.get(p).singleName).mkString("|")}->${singleName}"
    //}
  }

  def shortName: String = {
    RDD.shortName(name)
  }

  def singleName: String = {
    if (_name contains "adaptive") _name
    else _name + "-" + operator // default name for adaptive points
  }

  override def equals(_that: Any) = _that match {
    case RDD(stageId, id, _, _, _, _, _, _, _, _, _) => this.id == id
    case _ => false
  }

  def isAdaptable: Boolean = {
    // RDD.adaptableOps.contains(operator)
    RDD.adaptableOps.foreach { op =>
      if (operator.startsWith(op)) {
        return true
      }
    }
    false
  }

  //override def hashCode = (id, name).hashCode
  override def hashCode = (id, name).hashCode

  override def toString = {
    s"RDD(id=${id},scope=${scope},name=${shortName},operator=${operator})"
  }
}

object RDD extends Logging {
  var rdds: Map[Long,RDD] = Map.empty

  def shortName(name: String): String = {
    name
    //val names = name.split("->")
    //if (names.length == 1) {
    //  names(0)
    //} else {
    //  s"${names.head}/${names.length}/${names.last}"
    //}
  }

  /**
   * Enumeration of supported operations that support reconfiguration
   */
  val adaptableOps: Set[String] =
    Set("reduceByKey", "join", "aggregateByKey",
      "groupByKey", "textFile", "partitionBy", "repartition")
  
  def add(rdd: RDD) = {
    rdds += (rdd.id -> rdd)
  }

  def get(rddId: Long) = rdds(rddId)
}
