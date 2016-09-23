package br.ufmg.cs.systems.sparktuner.model

import br.ufmg.cs.systems.sparktuner.Analyzer._

import org.json4s._
import org.json4s.native.JsonMethods._

/**
 * Instances of this class hold information regarding RDDs identified in the
 * execution log. Also can represent an 'adaptation point', which is an
 * opportunity to change the application's parallelism.
 */
case class RDD(
    val id: Long,               // identifies the RDD
    val _name: String,          // identifies the adaptive point
    val scope: Long,            // some operators generate more than one RDD
    val operator: String,       // operator's name
    val parents: Array[Long],   // ids of parent RDDs
    val callSite: String        // contains the place in the code that this RDD was created
  ) {

  RDD.add (this)

  def this(jsonData: JValue) {
    this(
      getValue [Long] (jsonData, "RDD ID"),
      getValue [String] (jsonData, "Name"),
      getValue [String] (jsonData, "Scope").replaceAll ("\"|}|", "").
        replaceAll(",",":").split(":")(1).toLong,
      getValue [String] (jsonData, "Scope").replaceAll ("\"|}", "").split(":").last,
      getValue [Seq[BigInt]] (jsonData, "Parent IDs").map(_.toLong).toArray,
      getValue [String] (jsonData, "Callsite")
      )
  }

  def name = if (_name contains "adaptive") _name
  else _name + "-" + operator // default name for adaptive points

  override def equals(_that: Any) = _that match {
    case RDD(id, _, _, _, _, _) => this.id == id
    case _ => false
  }

  def isAdaptable: Boolean = RDD.adaptableOps contains operator

  override def hashCode = (id, name).hashCode

  override def toString = {
    s"RDD(id=${id},name=${name},operator=${operator},callsite=${callSite})"
  }
}

object RDD {
  var rdds: Map[Long,RDD] = Map.empty

  /**
   * Enumeration of supported operations that support reconfiguration
   */
  val adaptableOps: Set[String] =
    Set("reduceByKey", "join", "aggregateByKey", "textFile", "partitionBy")
  
  def add(rdd: RDD) = {
    rdds += (rdd.id -> rdd)
  }

  def get(rddId: Long) = rdds(rddId)
}
