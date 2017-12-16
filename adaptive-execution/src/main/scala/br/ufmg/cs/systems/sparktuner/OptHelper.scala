package br.ufmg.cs.systems.sparktuner

import br.ufmg.cs.systems.common.Logging
import br.ufmg.cs.systems.util.PrivateMethodExposer.p
import br.ufmg.cs.systems.sparktuner.Analyzer.PolicyFunc

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.serializer.Serializer
import org.apache.spark.Partitioner.defaultPartitioner

import scala.reflect.ClassTag


class JanusPartitioner(underlying: Partitioner) extends Partitioner {
  def numPartitions: Int = underlying.numPartitions
  def getPartition(key: Any): Int = underlying.getPartition(key)
}

/**
 * This class helps developers to include optmizations action in their code.
 */
class OptHelper(val analyzer: Analyzer = new Analyzer) extends Logging {

  lazy val apToActions: Map[String,Seq[Action]] = analyzer.getActions
  //lazy val apToActions: Map[String,Seq[Action]] = Map("ShuffledRDD-groupByKey at <console>:27" -> Seq(UNPAction("blah", 999)))

  private var currPos = Map.empty[String,Int].withDefaultValue (0)

  private def nextPos(point: String) = {
    val pos = currPos (point)
    if (pos < apToActions(point).size) {
      currPos += (point -> (pos+1))
      pos
    } else {
      logWarning (s"Execution plan is too small(${point},${pos},${currPos.size}), repeating last action.")
      apToActions.size - 1
    }
  }

  /**
   * Get a partitioner based on the current adaptative point.
   *
   * @param rdd the unadapted RDD
   * @param point the name that makes reference to an adaptive point
   *
   * @return spark partitioner adapted (or not) by the respective action
   */
  private def getPartitionerWithNext[K: ClassTag, V: ClassTag](
      rdd: RDD[(K,V)],
      point: String)
    (implicit orderingOpt: Option[Ordering[K]] = None): Partitioner =
    apToActions(point)(nextPos(point)) match {

    case act @ NOAction(ap) =>
      act.actionApplied(rdd.partitioner.get)

    case act @ UNPAction(ap, numPartitions) =>
      act.actionApplied (new HashPartitioner(numPartitions))

    case act @ UPAction(ap, "rangePartitioner") if orderingOpt.isDefined =>
      implicit val ordering = orderingOpt.get
      act.actionApplied (
        new RangePartitioner(rdd.partitioner.get.numPartitions,
          rdd.dependencies.head.rdd.asInstanceOf[RDD[_ <: Product2[K,V]]])
      )

    case act @ UPAction(ap, "hashPartitioner") =>
      act.actionApplied (new HashPartitioner(rdd.partitioner.get.numPartitions))

    case action =>
      throw new RuntimeException (s"Unrecognized Action: ${action}")
  }

  @scala.annotation.tailrec
  private def findAP(point: String)(altAdptName: String)
      : String = apToActions.get(point) match {
    case None =>
      if (altAdptName != null) {
        logInfo (s"Not found AP ${point}, trying ${altAdptName} instead?")
        findAP(altAdptName)(null)
      } else {
        logInfo (s"Alternative AP ${altAdptName} not found")
        null
      }

    case _ =>
      logInfo (s"Found actions for AP ${point}")
      point
  }

  /**
   * Get a partitioner based on the current adaptative point.
   *
   * @param prev the RDD that will originate an RDD representing an adaptive
   * point
   * @param point the name that makes reference to an adaptive point
   *
   * @return spark partitioner adapted (or not) by the respective action
   */
  def getPartitioner[K: Ordering: ClassTag, V: ClassTag](
      point: String,
      defaultNumPartitions: Int = -1,
      prev: RDD[(K,V)] = null)(implicit altAdptName: String = null)
    : Partitioner = new JanusPartitioner(apToActions.get(findAP(point)(altAdptName)) match {
    
    case Some(actions) => actions(nextPos(findAP(point)(altAdptName))) match {
      case act @ NOAction(ap) if defaultNumPartitions == -1 =>
        act.actionApplied (defaultPartitioner (prev))
      
      case act @ WarnAction(ap, msg) if defaultNumPartitions == -1 =>
        logWarning (s"Found warning for AP ${ap}: ${act}")
        act.actionApplied (defaultPartitioner (prev))
      
      case act @ WarnAction(ap, msg) =>
        logWarning (s"Found warning for AP ${ap}: ${act}")
        act.actionApplied (new HashPartitioner(defaultNumPartitions))
      
      case act @ NOAction(ap) =>
        act.actionApplied (new HashPartitioner(defaultNumPartitions))

      case act @ UNPAction(ap, numPartitions) =>
        act.actionApplied (new HashPartitioner(numPartitions))

      case act @ UPAction(ap, "rangePartitioner") =>
        act.actionApplied (
          new RangePartitioner(defaultPartitioner(prev).numPartitions, prev)
        )

      case act @ UPAction(ap, "hashPartitioner") =>
        act.actionApplied (new HashPartitioner(defaultPartitioner(prev).numPartitions))

      case action =>
        throw new RuntimeException (s"Unrecognized Action: ${action}")
    }

    case None if defaultNumPartitions == -1 =>
      defaultPartitioner(prev)

    case None =>
      new HashPartitioner(defaultNumPartitions)
  })

  def getNumPartitions(
      defaultNumPartitions: Int,
      point: String)(implicit altAdptName: String = null)
    : Int = apToActions.get(findAP(point)(altAdptName)) match {

    case Some(actions) => actions(nextPos(findAP(point)(altAdptName))) match {
      case act @ NOAction(ap) => act.actionApplied(defaultNumPartitions)
      
      case act @ WarnAction(ap, msg) =>
        logWarning (s"Warning action: ${act}")
        act.actionApplied(defaultNumPartitions)

      case act @ UNPAction(ap, numPartitions) => act.actionApplied (numPartitions)

      case act @ UPAction(_, _) => act.actionApplied (defaultNumPartitions)

      case action =>
        throw new RuntimeException (s"Unrecognized Action: ${action}")
    }
    
    case None => defaultNumPartitions

  }

  /**
   * Users call this function to get an potentially adapted RDD if actions are
   * associated to it
   *
   * @param point the adaptive point name
   * @param rdd RDD that must be adapted
   * @param prev RDD that originated `rdd` through an operator (e.g. reduceByKey
   * and join)
   *
   * @return adapted RDD or the same RDD if that could not be done
   */
  def adaptRDD[K: ClassTag, V: ClassTag, C: ClassTag](
      point: String,
      rdd: RDD[(K,C)],
      prev: RDD[(K,V)]): RDD[(K,C)] = rdd match {
    case shRdd: ShuffledRDD[K,V,C] =>
      adaptShuffledRDD (point, shRdd, shRdd.prev).
        setName (point)
    case cgRdd: CoGroupedRDD[K] => 
      adaptCoGroupedRDD (point, cgRdd)
    case _ =>
      rdd.setName (point)
  }

  // TODO: create adapt functions for other common RDDs that could represent a
  // adaptive point

  /** adapt CoGroupedRDD */
  private def adaptCoGroupedRDD[K: ClassTag](point: String,
      rdd: CoGroupedRDD[K]) = apToActions.get (point) match {
    case Some(actions) =>
      val cgRdd = new CoGroupedRDD[K](rdd.rdds, getPartitionerWithNext(rdd, point)).
        setSerializer (p(rdd)('serializer)().asInstanceOf[Serializer])
      cgRdd
    case None =>
      rdd
  }
  
  /** adapt ShuffledRDD */
  private def adaptShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
      point: String, rdd: ShuffledRDD[K,V,C],
      prev: RDD[_ <: Product2[K,V]]) = apToActions.get (point) match {

    case Some(actions) =>

      implicit val keyOrderingOpt = p(rdd)('keyOrdering)().asInstanceOf[Option[Ordering[K]]]
      
      val shRdd = new ShuffledRDD[K,V,C](rdd.prev, getPartitionerWithNext(rdd, point)).
        setSerializer (
          p(rdd)('serializer)().asInstanceOf[Option[Serializer]].getOrElse(null)
        ).
        setKeyOrdering (keyOrderingOpt.getOrElse(null)).
        setAggregator (
          p(rdd)('aggregator)().asInstanceOf[Option[Aggregator[K,V,C]]].getOrElse(null)
        ).
        setMapSideCombine (
          p(rdd)('mapSideCombine)().asInstanceOf[Boolean]
        )
      shRdd

    case None =>
      rdd
  }

  override def toString = s"OptHelper(numActions=${apToActions.size})"
}

object OptHelper extends Logging {

  private var _instances: Map[SparkConf,OptHelper] = Map.empty

  def get(conf: SparkConf,
      extraPolicies: Seq[(String,PolicyFunc)] = Seq.empty)
    : OptHelper = _instances.get (conf) match {
    case Some(oh) =>
      extraPolicies.foreach (p => oh.analyzer.addPolicy (p._1, p._2))
      oh

    case None =>
      val logPath = conf.get ("spark.adaptive.logpath", null)
      val oh = if (logPath == null || logPath.isEmpty) {
        new OptHelper
      } else {
        val analyzer = new Analyzer (logPath)
        extraPolicies.foreach {p =>
          analyzer.addPolicy(p._1, p._2)
          logInfo (s"Including policy: ${p._1}")
        }
        new OptHelper (analyzer)
      }
      logInfo (s"${oh} created for ${conf}")
      _instances += (conf -> oh)
      oh
  }

}
