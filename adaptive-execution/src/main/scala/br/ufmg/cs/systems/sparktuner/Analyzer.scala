package br.ufmg.cs.systems.sparktuner

import br.ufmg.cs.systems.common.Logging
import br.ufmg.cs.systems.sparktuner.model._

import org.json4s._
import org.json4s.native.JsonMethods._

import org.apache.commons.math3.stat.correlation.{PearsonsCorrelation, SpearmansCorrelation}
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.commons.math3.stat.descriptive.moment.Skewness

import scala.io.Source
import java.net.URI
import java.nio.file.Paths
  
class Analyzer(logPath: String = "",
    corrThreshold: Double = 0.8,
    skewnessThreshold: Double = 1.5) extends Logging {
  import Analyzer._

  logInfo (s"Analyzer initialized from ${logPath}")
  
  // parse log file (fill model)
  private val stages: Seq[(RDD,List[Stage])] = groupByRDD (parseLogFile).
    toSeq.sortBy (_._1.id)

  private var dependencies: Map[String,Option[String]] = _

  /** policies for optimization **/
  private var _policies: Seq[(String,PolicyFunc)] = Seq(
    ("empty-tasks", optForEmptyTasks),
    ("spill", optForSpill),
    ("garbage-collection", optForGc),
    ("task-imbalance", optForTaskImbalance)
    //("fetch-wait-time", optForFetchWaitTime),
    )

  /**
   * Users can add their custom policies through this call. The default policies
   * are:
   * - Empty Tasks:
   * - Spill:
   * - Garbage Collection:
   * - Task Imbalance: 
   *
   * @param func new policy receives the RDD representing an adaptive point and
   * a list of respective stages starting in this AP and returns an [[Action]]
   */
  def addPolicy(name: String, func: PolicyFunc): Unit = {
    _policies = _policies :+ ( (name, func) )
  }

  def policies = _policies

  private def skewness(values: Array[Long]): Double =
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
  
  private def getRepr(stages: List[Stage]): Stage =
    stages.maxBy (_.id)

  def getActions: Map[String,Seq[Action]] = {
    val iteratives = isIterative
    val regulars = isRegular

    logInfo (s"iteratives: ${iteratives}")
    logInfo (s"regulars: ${regulars}")

    // separate by categories
    var apPoints = Map.empty[String,Seq[(RDD,List[Stage])]].
      withDefaultValue (Seq.empty[(RDD,List[Stage])])

    for ((rdd,stages) <- stages) {
      apPoints += (rdd.name -> ((rdd,stages) +: apPoints(rdd.name)))
    }

    // iterate over all adaptive points and instances
    var allActions = Map.empty[String,Seq[Action]]
    for ((ap,instances) <- apPoints) {
      val iterative = iteratives(ap)
      val regular = regulars(ap)
      val sorted_instances = instances.sortBy (kv => kv._1.id)
      // decide and optimize based on the application type
      (iterative, regular) match {
        case (true, true) => // iterative and regular
          val actions = optIterativeRegular (sorted_instances)
          allActions = allActions ++ actions

        case (true, false) => // iterative and irregular
          val actions = optIterativeIrregular (sorted_instances)
          allActions = allActions ++ actions

        case (false, _) => // noniterative
          val actions = optNoniterative (sorted_instances)
          allActions = allActions ++ actions

        case _ =>
          throw new RuntimeException (s"Could not determine the AP(${ap}) category")
      }
    }

    handleDependencies(allActions).map {case (ap,actions) => (ap,actions :+ NOAction("wildcard"))}
  }

  private def handleDependencies(allActions: Map[String,Seq[Action]]) = {
    allActions.map { case (ap,actions) => dependencies(ap) match {
      case Some(dep) =>
        (ap ->
          actions.zip (allActions(dep)).map {case (a1,a2) => a1 max a2})
      case None =>
        (ap -> actions)
    }
    }
  }

  private def optNoniterative(instances: Seq[(RDD,List[Stage])])
      : Map[String,Seq[Action]] = {
    var actions = Map.empty[String,Seq[Action]].withDefaultValue (Seq.empty[Action])
    
    for ((rdd,stages) <- instances) {
      val action = optAP (rdd, stages)
      actions += (rdd.name -> (actions(rdd.name) :+ action))
    }

    actions
  }

  private def optIterativeRegular(instances: Seq[(RDD,List[Stage])])
      : Map[String,Seq[Action]] = {
    var actions = Map.empty[String,Seq[Action]].
      withDefaultValue (Seq.empty[Action])
    
    var numRdds = 0
    for ((rdd,stages) <- instances) (actions.get (rdd.name), getRepr(stages)) match {
      case (Some(apActions), repr) if repr.rdds.size == numRdds =>
        actions += (rdd.name -> (apActions :+ apActions.last))
        numRdds = repr.rdds.size
      
      case (Some(apActions), repr) =>
        val _actions = optNoniterative (Seq((rdd,stages)))
        val newAction = _actions.values.head.head
        val lastAction = apActions.last.max (newAction)
        actions += (rdd.name -> (apActions.dropRight(1) :+ lastAction :+ lastAction))
        numRdds = repr.rdds.size


      case (None, repr) =>
        val _actions = optNoniterative (Seq((rdd,stages)))
        actions += (rdd.name -> _actions.values.head)
        numRdds = repr.rdds.size
    }

    actions
  }

  private def optIterativeIrregular(instances: Seq[(RDD,List[Stage])])
      : Map[String,Seq[Action]] = {
    var actions = Map.empty[String,Seq[Action]]
    var firstInput = Map.empty[String,Long]

    for ((rdd,stages) <- instances)
        (actions.get(rdd.name), firstInput.get(rdd.name)) match {
      case (Some(apActions), Some(fi)) => // scale optimization
        val repr = getRepr (stages)
        val factor = repr.recordsRead / fi.toDouble
        val firstAction = apActions.head
        val action = firstAction.scaled (factor)
        actions += (rdd.name -> (apActions :+ action))

      case _ => // first iteration
        val repr = getRepr (stages)
        val _actions = optNoniterative (Seq((rdd,stages)))
        val newActions = _actions.values.head match {
          case NOAction(ap) +: tail =>
            UNPAction (ap, repr.numTasks.toInt).
              setOldNumPartitions (repr.numTasks.toInt) +: tail
          case acts => acts
        }
        actions += (rdd.name -> newActions)
        firstInput += (rdd.name -> repr.recordsRead)
    }

    actions
  }

  private def optAP(rdd: RDD, stages: List[Stage]): Action = {
    for ((name,optFunc) <- policies.iterator) {
      val action = optFunc (rdd, stages)
      if (action.valid) {
        return action.
          setPolicySrc (name).
          setOldNumPartitions (getRepr(stages).numTasks.toInt)
      }
    }
    val action = NOAction (rdd.name)
    action
  }


  /** Default policies */
  private def optForEmptyTasks(rdd: RDD, stages: List[Stage]): Action = {
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

  private def optForSpill(rdd: RDD, stages: List[Stage]): Action = {
    logInfo (s"${rdd}: optimizing for spill")
    val repr = getRepr (stages)
    val runTimes = repr.taskRunTimes
    val bytesSpilled = repr.taskBytesSpilled
    val shuffleWriteBytes = repr.shuffleWriteBytes
    if (shuffleWriteBytes > 0) {
      val factor = repr.bytesSpilled / shuffleWriteBytes.toDouble
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

  private def optForFetchWaitTime(rdd: RDD, stages: List[Stage]): Action = {
    logInfo (s"${rdd}: optimizing for Fetch Wait Time")
    val repr = getRepr (stages)
    val fetchWaitTimes = repr.taskFetchWaitTimes
    val blockedReadOverheads = repr.taskBlockedReadOverheads
    val runTimes = repr.taskRunTimes

    val above = blockedReadOverheads.filter(_ > 0.8)

    if (!above.isEmpty) {

      val newNumPartitions = repr.numTasks + above.size*2
      UNPAction (rdd.name, newNumPartitions.toInt)

    } else {
      NOAction (rdd.name)
    }
  }

  private def optForGc(rdd: RDD, stages: List[Stage]): Action = {
    logInfo (s"${rdd}: optimizing for GC")
    val repr = getRepr (stages)
    val runTimes = repr.taskRunTimes
    val gcTimes = repr.taskGcTimes
    val gcOverheads = repr.taskGcOverheads
    val _skewness = skewness (gcOverheads)
    percentileCalc.setData (gcOverheads)

    val q2 = percentileCalc.evaluate (50)
    val q1 = percentileCalc.evaluate (25)
    val q3 = percentileCalc.evaluate (75)
    val percentiles = List(q2, q3).filter (_ > 0)
    
    if (highSkewness (_skewness) && !percentiles.isEmpty) {

      val target = percentiles.min
      val newNumPartitions = gcOverheads.map (go => math.ceil(go / target).toInt max 1).sum
      UNPAction (rdd.name, newNumPartitions)

    } else {
      NOAction (rdd.name)
    }
  }

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

  private def optForTaskImbalance(rdd: RDD, stages: List[Stage]): Action = {
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

  private def isIterative: Map[String,Boolean] = {
    var iterative = Map.empty[String,Int].withDefaultValue (0)
    for ((rdd,stages) <- stages.iterator)
      iterative += (rdd.name -> (iterative(rdd.name) + 1))
    iterative.mapValues (_ > 1)
  }

  private def isRegular: Map[String,Boolean] = {
    var regular = Map.empty[String,Map[String,Long]]

    for ((rdd,stages) <- stages.iterator) {
      val recordsRead = stages.map (s => (s.name,s.recordsRead)).toMap

      regular.get (rdd.name) match {
        case Some(_recordsRead) if _recordsRead.isEmpty =>
          // keep going, irregular

        case Some(_recordsRead) if equivInputs (recordsRead, _recordsRead) =>
          regular += (rdd.name -> _recordsRead)

        case Some(_recordsRead) if equivInputs (_recordsRead, recordsRead) =>
          regular += (rdd.name -> recordsRead)

        case Some(_) =>
          regular += (rdd.name -> Map.empty[String,Long])

        case None =>
          regular += (rdd.name -> recordsRead)
      }
    }

    regular.mapValues (!_.isEmpty)
  }

  private def groupByRDD(stages: Map[Long,Stage]) = {
    var grouped = Map.empty[RDD,List[Stage]].withDefaultValue (Nil)

    dependencies = Map.empty[String,Option[String]].withDefaultValue (None)

    for ((stageId,stage) <- stages.iterator) if (stage.canAdapt) {
      val adptRDD = stage.adaptiveRDD
      grouped += (adptRDD -> (stage :: grouped(adptRDD)))

      val possibleDeps = stage.rdds.filter (_ != adptRDD)
      if (!possibleDeps.isEmpty) {
        val firstRDD = possibleDeps.minBy (_.id)
        if (adptRDD.scope != firstRDD.scope &&
              firstRDD.isAdaptable && (grouped.keys.toSet contains firstRDD)) {
          // two way
          dependencies += (adptRDD.name -> Some(firstRDD.name))
          dependencies += (firstRDD.name -> Some(adptRDD.name))
        }
      }
    }

    grouped
  }

  private def parseLogFile: Map[Long,Stage] = {
    var stages = Map.empty[Long,Stage]
    if (logPath.isEmpty)
      return stages

    val source = try {
      Source.fromFile (new URI(logPath))
    } catch {
      case _: java.io.FileNotFoundException | _: java.lang.IllegalArgumentException =>
        Source.fromFile (Paths.get(logPath).toUri)
      case e: Throwable =>
        throw e
    }

    for (line <- source.getLines) {
      val strInput: JsonInput = new StringInput(line)
      val jsonData = parse (strInput, false, false)

      jsonData \ "Event" match {

        case JString("SparkListenerTaskEnd") =>
          val task = new Task(jsonData)
          stages(task.stageId).addTask (task)

        case JString("SparkListenerStageSubmitted") =>
          val stage = new Stage(jsonData)
          if (!(stages contains stage.id))
            stages += (stage.id -> stage)

        case _ =>
      }
    }

    stages
  }

}

object Analyzer extends Logging {

  /**
   * Used for overall estimations
   */
  lazy val pCorrelation = new PearsonsCorrelation
  lazy val sCorrelation = new SpearmansCorrelation
  lazy val percentileCalc = new Percentile
  lazy val skewnessCalc = new Skewness

  /**
   * This is the signature that a custom function must have in order to be used
   * as a reconfiguration policy.
   */
  type PolicyFunc = (RDD,List[Stage]) => Action

  def main (args: Array[String]) {
    val logPath = args(0)
    val actions = new Analyzer (logPath).getActions
    logInfo (
      s"Actions: ${actions.map (kv => s"${kv._1}\n\t${kv._2.mkString("\n\t")}").mkString("\n")}")
  }

  /**
   * Get values in json4s form
   */
  @scala.annotation.tailrec
  def getJValue(jsonData: JValue, keys: String*): JValue = {
    val _jsonData = jsonData \ keys(0)
    val _keys = keys.drop (1)
    if (!_keys.isEmpty)
      getJValue (_jsonData, _keys:_*)
    else
      _jsonData
  }

  /**
   * Get value as type 'T'
   */
  def getValue [T] (jsonData: JValue, keys: String*): T = {
    getJValue (jsonData, keys:_*) match {
      case JNothing =>
        null.asInstanceOf[T]
      case JInt(bi : BigInt) =>
        bi.toLong.asInstanceOf[T]
      case jvalue =>
        jvalue.values.asInstanceOf[T]
    }
  }

  /**
   * Returns true if 'superset' contains all elements from 'subset'.
   * We consider fairly constant stages instead exactly equal ones 
   * to account for small measure deviations.
   */
  def equivInputs (subset: Map[String,Long], superset: Map[String,Long]): Boolean = {
    for ((k,v) <- subset) superset.get(k) match {
      case Some(_v) if _v > v =>
        if ( (v / _v.toDouble) < 0.9 ) // TODO: not fairly constant
          return false
      case Some(_v) =>
        if ( (_v / v.toDouble) < 0.9 ) // TODO: not fairly constant
          return false
      case None =>
    }
    true
  }

}
