package br.ufmg.cs.systems.sparktuner.model

import br.ufmg.cs.systems.sparktuner.Analyzer._

import org.json4s._
import org.json4s.native.JsonMethods._

/**
 * Instances of this class hold information about an observed stage in the
 * execution. Stages are also entrance points for reading and interpreting
 * aggregated statistics about the execution.
 */
class Stage(
    val id: Long,                   // identifies the stage within an execution 
    val name: String,               // stage name provided by the framework
    val numTasks: Long,             // number of tasks in this stage
    val tasks: Array[Task],         // array of tasks (for individual statistics)   
    val parents: Array[Long],       // ids of parent Stages
    val rdds: Array[RDD],           // list of RDDs contained in this stage (exec. pipeline)
    onlyAdaptive: Boolean = false   // mechanism for excluding some points from the opt. process
  ) {

  Stage.add(this)

  rdds.foreach (_.setStage(this))

  def this(jsonData: JValue) {
    this(
      getValue [Long] (jsonData, "Stage Info", "Stage ID"),
      getValue [String] (jsonData, "Stage Info", "Stage Name"),
      getValue [Long] (jsonData, "Stage Info", "Number of Tasks"),
      Array.fill (getValue [Long] (jsonData, "Stage Info", "Number of Tasks").toInt) (Task.empty),
      getValue [Seq[BigInt]] (jsonData, "Stage Info", "Parent IDs").map (_.toLong).toArray,
      getJValue (jsonData, "Stage Info", "RDD Info").children.
        map (rddData => new RDD(
          getValue [Long] (jsonData, "Stage Info", "Stage ID"),
          rddData)
        ).toArray
      )
  }

  def addTask(task: Task) = {
    tasks(task.idx.toInt) = task
  }

  def firstRDD: RDD = {
    rdds.minBy (_.id)
  }
  
  def adaptiveRDD: RDD = {
    // println (s"%%% ${id} rdds: ${rdds.mkString(", ")}")
    val adaptables = rdds.filter (_.isAdaptable)
    // println (s"%%% ${id} adaptables: ${adaptables.mkString(", ")}")
    val maxScope = adaptables.map(_.scope).max
    // println (s"%%% ${id} maxScope: ${maxScope}")
    val ardd = adaptables.filter(_.scope == maxScope).minBy (_.id)
    // println (s"%%% ${id} adaptiveRDD: ${ardd}")
    // println ()
    // println ()
    // println ()
    ardd
  }

  def adaptiveRDDs: Array[RDD] = {
    val adaptables = rdds.filter (_.isAdaptable)

    var adaptablesByScope = Map.empty[Long,RDD]
    adaptables.foreach { rdd =>
      adaptablesByScope.get(rdd.scope) match {
        case Some(_rdd) =>
          adaptablesByScope += (rdd.scope -> Seq(rdd, _rdd).minBy(_.id))
        case None =>
          adaptablesByScope += (rdd.scope -> rdd)
      }
    }

    adaptablesByScope.values.toArray
  }

  def emptyTasks: Array[Task] = tasks.filter (_.recordsRead == 0)

  def adaptivePoint: String = {
    val frdd = adaptiveRDD
    if (frdd.name contains "adaptive")
      frdd.name
    else
      "-"
  }

  def useMem: Boolean = {
    rdds.foreach { rdd =>
      if (rdd.useMem) {
        return true
      }
    }
    false
  }

  def cachedParents: Boolean = {
    parents.foreach { stageId =>
      Stage.getOpt(stageId) match {
        case Some(parent) =>
          if (parent.useMem || parent.cachedParents) {
            return true
          }

        case None =>
      }
    }
    false
  }

  def canAdapt: Boolean = {
    //!cachedParents && (!onlyAdaptive || (adaptivePoint contains "adaptive"))
    (!onlyAdaptive || (adaptivePoint contains "adaptive"))
  }

  // statistics derived from tasks

  def taskRunTimes: Array[Long] = tasks.map (_.runTime)

  def taskGcTimes: Array[Long] = tasks.map (_.gcTime)
  
  def taskGcOverheads: Array[Double] = tasks.map (_.gcOverhead)

  def taskShuffleReadBytes: Array[Long] = tasks.map (_.shuffleReadBytes)

  def taskShuffleReadRecords: Array[Long] = tasks.map (_.shuffleReadRecords)
  
  def taskFetchWaitTimes: Array[Long] = tasks.map (_.fetchWaitTime)
  
  def taskBlockedReadOverheads: Array[Double] = tasks.map (_.blockedReadOverhead)

  def taskBytesSpilled: Array[Long] = tasks.map (_.bytesSpilled)

  def inputRecords: Long = tasks.map (_.inputRecords).sum
  
  def inputBytes: Long = tasks.map (_.inputBytes).sum

  def shuffleReadRecords: Long = tasks.map (_.shuffleReadRecords).sum

  def shuffleReadBytes: Long = tasks.map (_.shuffleReadBytes).sum

  def shuffleWriteRecords: Long = tasks.map (_.shuffleWriteRecords).sum

  def shuffleWriteBytes: Long = tasks.map (_.shuffleWriteBytes).sum

  def bytesSpilled: Long = tasks.map (_.bytesSpilled).sum

  def recordsRead: Long = inputRecords + shuffleReadRecords
  
  def recordsReadBytes: Long = inputBytes + shuffleReadBytes
  
  def bytesRead: Long = inputRecords + shuffleReadBytes

  def taskGcTimesPerExecutor: Map[String,Seq[Long]] =
    tasks.map (t => (t.executorId, t.gcTime)).groupBy (_._1).mapValues (_.unzip._2)
  
  def taskGcOverheadsPerExecutor: Map[String,Seq[Double]] =
    tasks.map (t => (t.executorId, t.gcOverhead)).groupBy (_._1).mapValues (_.unzip._2)

  override def toString = {
    s"Stage(id=${id},numTasks=${tasks.size},name=${name})"
  }

}

object Stage {
  var stages: Map[Long,Stage] = Map.empty

  def add(stage: Stage) = {
    stages += (stage.id -> stage)
  }

  def get(stageId: Long) = stages(stageId)
  
  def getOpt(stageId: Long) = stages.get(stageId)

}
