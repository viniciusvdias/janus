package br.ufmg.cs.systems.sparktuner.model

import br.ufmg.cs.systems.common.Logging
import br.ufmg.cs.systems.sparktuner._

import org.apache.commons.math3.stat.correlation.{PearsonsCorrelation, SpearmansCorrelation}
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.commons.math3.stat.descriptive.moment.Skewness

import org.json4s._
import org.json4s.native.JsonMethods._

import Policies._

/**
 */
trait ApplicationPolicy extends Logging {
  def beforeExecution(env: Environment): Action = 
    NOAction("No application action configured")
}

trait PartitioningPolicy extends ApplicationPolicy {
  def adapt(env: Environment, apStats: AdaptivePointStats): Action
}

/**
 * Common functions used on some policies
 */
object Policies {
  val skewnessThreshold: Double = 1.5
  val corrThreshold: Double = 0.8

  /**
   * Used for overall estimations
   */
  lazy val pCorrelation = new PearsonsCorrelation
  lazy val sCorrelation = new SpearmansCorrelation
  lazy val percentileCalc = new Percentile
  lazy val skewnessCalc = new Skewness

  def getRepr(stages: List[Stage]): Stage =
    stages.maxBy (_.id)

  def skewness(values: Array[Long]): Double =
    skewness (values.map(_.toDouble))

  def skewness(values: Array[Double]): Double =
    skewnessCalc.evaluate (values)

  def highSkewness(skewness: Double): Boolean = {
    skewness > skewnessThreshold
  }

  def correlation(values1: Array[Long], values2: Array[Long]): Double = {
    if (values1.sum == 0 || values2.sum == 0)
      0.0
    else {
      pCorrelation.correlation (values1.map(_.toDouble),
        values2.map(_.toDouble)) match {
          case corr if corr.isNaN => 1.0
          case corr => corr
      }
    }
  }

  def highCorrelation(corr: Double): Boolean = {
    corr.abs >= corrThreshold
  }
}

object LocalityPolicy extends ApplicationPolicy {

  private val replFactor: Int = 3
  private val localityTarget: Double = 0.9

  private def ln(n: Double): Double = {
    scala.math.log(n) / scala.math.log(scala.math.E)
  }

  private def stageDelay(
      numExecutors: Int,
      coresPerExecutor: Int,
      stage: Stage
    ): Double = {
    val taskRunTimes = stage.taskRunTimes
    val avgTaskLength: Double = (taskRunTimes.sum / taskRunTimes.size.toDouble)
    val minSchedDelay = - (numExecutors / replFactor) * ln( (1 - localityTarget) / (1 + (1 - localityTarget)) )
    (minSchedDelay / coresPerExecutor) * avgTaskLength
  }

  override def beforeExecution(env: Environment): Action = {
    val numExecutors: Int = env.executors.size
    val coresPerExecutor: Int = (env.executors.map (_.totalCores).sum / numExecutors).toInt
    val schedDelay: Double = env.stages.map (stage => stageDelay(numExecutors, coresPerExecutor, stage)).max
    UConfAction("spark.locality.wait", s"${schedDelay/1000}s")
  }
}

object EmptyTasksPolicy extends PartitioningPolicy {
  override def adapt(env: Environment, apPoints: AdaptivePointStats): Action = {
    val stages = apPoints.stages
    val rdd = apPoints.rdd
    logInfo (s"${rdd}: optimizing for emptyTasks")
    val repr = getRepr (stages)
    val emptyTasks = repr.emptyTasks
    val numEmptyTasks = emptyTasks.size
    if (numEmptyTasks > 0) {
      UNPAction (rdd.name, (repr.numTasks - numEmptyTasks).toInt)
    } else {
      NOAction (rdd.name)
    }
  }
}

object SpillPolicy extends PartitioningPolicy {
  override def adapt(env: Environment, apPoints: AdaptivePointStats): Action = {
    val stages = apPoints.stages
    val rdd = apPoints.rdd
    
    logInfo (s"${rdd}: optimizing for spill")
    val repr = getRepr (stages)
    val runTimes = repr.taskRunTimes
    val bytesSpilled = repr.bytesSpilled
    val shuffleWriteBytes = repr.shuffleWriteBytes

    if (shuffleWriteBytes > 0) {
      val factor = bytesSpilled / shuffleWriteBytes.toDouble
      if (factor > 0) {
        val newNumPartitions = repr.numTasks + math.ceil (factor * repr.numTasks)
        UNPAction (rdd.name, newNumPartitions.toInt)
      } else {
        NOAction (rdd.name)
      }
    } else {
      NOAction (rdd.name)
    }
  }
}

object GCPolicy extends PartitioningPolicy {

  override def adapt(env: Environment, apPoints: AdaptivePointStats): Action = {
    val stages = apPoints.stages
    val rdd = apPoints.rdd
    
    logInfo (s"${rdd}: optimizing for GC")
    val repr = getRepr (stages)
    val gcOverheads = repr.taskGcOverheads
    val _skewness = skewness (gcOverheads)

    if (highSkewness (_skewness)) {

      val target = percentileCalc.evaluate (50)
      val newNumPartitions = gcOverheads.map (go => math.ceil(go / target).toInt max 1).sum
      UNPAction (rdd.name, newNumPartitions)

    } else {
      NOAction (rdd.name)
    }
  }
}

object TaskImbalancePolicy extends PartitioningPolicy {
  /**
   * The following classes represent the possible causes for task imbalance.
   * These facts help us to determine the best set of actions to take
   */
  private sealed trait SourceOfImbalance
  private case object NoImbalance extends SourceOfImbalance
  private case object KeyDist extends SourceOfImbalance
  private case object Inherent extends SourceOfImbalance
  private case object VariableCost extends SourceOfImbalance

  private def sourceOfImbalance(stage: Stage): SourceOfImbalance = {
    val runTimes = stage.taskRunTimes
    val _skewness = skewness (runTimes)
    logInfo (s"Imbalance skewness ${_skewness}")
    if (!highSkewness(_skewness)) return NoImbalance


    val corr1 = correlation (runTimes, stage.taskShuffleReadBytes)
    val corr2 = correlation (runTimes, stage.taskShuffleReadRecords)
    logInfo (s"Correlation between Runtime and ShuffleReadBytes: ${corr1}")
    logInfo (s"Correlation between Runtime and ShuffleReadRecords: ${corr2}")
    (highCorrelation (corr1), highCorrelation (corr2)) match {
      case (true, true) => KeyDist
      case (false, false) => Inherent
      case _ => VariableCost
    }
  }

  override def adapt(env: Environment, apPoints: AdaptivePointStats): Action = {
    val stages = apPoints.stages
    val rdd = apPoints.rdd
    logInfo (s"${rdd}: optimizing for taskImbalance")
    val repr = getRepr (stages)
    sourceOfImbalance (repr) match {
      case Inherent =>
        WarnAction (rdd.name, Inherent.toString)

      case KeyDist =>
        UPAction (rdd.name, "rangePartitioner")

      case VariableCost =>
        WarnAction (rdd.name, VariableCost.toString)
        
      case NoImbalance =>
        NOAction (rdd.name)
    }
  }
  /*****/

}
