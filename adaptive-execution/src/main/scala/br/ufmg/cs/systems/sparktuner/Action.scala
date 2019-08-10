package br.ufmg.cs.systems.sparktuner

import br.ufmg.cs.systems.common.Logging
import br.ufmg.cs.systems.util.PrivateMethodExposer.p
import br.ufmg.cs.systems.sparktuner.Analyzer.PolicyFunc
import br.ufmg.cs.systems.sparktuner.model.RDD

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.serializer.Serializer
import org.apache.spark.Partitioner.defaultPartitioner

import scala.reflect.ClassTag

sealed trait Action extends Logging {
  // we consider an RDD as possible adaptive points
  def ap: String

  // some action may be augmented or diminished by a factor
  def scaled(factor: Double): Action = this

  // only 'NoAction' by default is considered a non valid action
  def valid = true

  def actionApplied [T] (res: T): T = {
    logInfo (s"${this}:${policySrc} was applied and produced: ${res}")
    res
  }

  def max(other: Action): Action = this

  def min(other: Action): Action = {
    max(other) match {
      case _max if _max == this =>
        other
      case _ =>
        this
    }
  }

  private var _policySrc: String = ""

  /**
   * Policy name that originated this action
   */
  def policySrc: String = _policySrc

  def setPolicySrc(os: String): Action = {
    _policySrc = os
    this
  }

  private var _oldNumPartitions: Int = _
  private var _oldPartitioner: String = _

  def oldNumPartitions: Int = _oldNumPartitions
  def oldPartitioner: String = _oldPartitioner

  def setOldNumPartitions(onp: Int): Action = {
    _oldNumPartitions = onp
    this
  }

  def setOldPartitioner(op: String): Action = {
    _oldPartitioner = op
    this
  }

  override def toString = {
    s"(${policySrc}" +
    s"${Option(oldNumPartitions).filter(_ > 0).map(n => "," + n.toString).getOrElse("")}" +
    s"${Option(oldPartitioner).map(p => "," + p).getOrElse("")})"
  }
}

/**
 * Applied when the number of partitions of an adaptive point must change based
 * on some criteria.
 */
case class UNPAction(ap: String, numPartitions: Int) extends Action {
  override def scaled(factor: Double): Action = {
    val newNumPartitions = math.ceil (factor * numPartitions).toInt max 1
    UNPAction (ap, newNumPartitions).
      setPolicySrc (policySrc).
      setOldNumPartitions (oldNumPartitions).
      setOldPartitioner (oldPartitioner)
  }

  override def max(_other: Action): Action = _other match {
    case other: UNPAction => List(this, other).maxBy (_.numPartitions)
    case _ => super.max(_other)
  }

  override def toString = {
    s"UNPAction(${RDD.shortName(ap)},${numPartitions})${super.toString}"
  }
}

/**
 * Applied when the partitioning strategy of some adaptive point must be
 * changed, like hashPartitioner -> rangePartitioner
 */
case class UPAction(ap: String, partitionerStr: String) extends Action {
  override def toString = {
    s"UPAction(${RDD.shortName(ap)},${partitionerStr})${super.toString}"
  }
}

case class UConfAction(key: String, value: String) extends Action {
  def ap: String = throw new RuntimeException("Adaptation Point not allowed in UConfAction")
  override def toString = {
    s"UConfAction(${key},${value})${super.toString}"
  }
}


/**
 * Applied when there is nothing else to do regarding this adaptive point
 */
case class NOAction(ap: String) extends Action {
  override def valid = false

  override def max(other: Action): Action = other

  override def toString = {
    s"NOAction(${RDD.shortName(ap)})${super.toString}"
  }
}

/**
 * This action only carry a warn message that is displayed to the user if
 * activated. We choose such approach in cases where no automatic
 * reconfiguration is known. Usually this means that we need an user direct
 * intervention.
 */
case class WarnAction(ap: String, msg: String) extends Action {
  override def valid = !msg.isEmpty

  override def max(other: Action): Action = other

  override def toString = {
    s"WarnAction(${RDD.shortName(ap)},${msg})${super.toString}"
  }
}
