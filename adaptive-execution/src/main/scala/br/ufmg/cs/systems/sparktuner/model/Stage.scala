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
    val parents: Array[Long],       // ids of parent RDDs
    val rdds: Array[RDD],           // list of RDDs contained in this stage (exec. pipeline)
    onlyAdaptive: Boolean = false   // mechanism for excluding some points from the opt. process
  ) {

  def this(jsonData: JValue) {
    this(
      getValue [Long] (jsonData, "Stage Info", "Stage ID"),
      getValue [String] (jsonData, "Stage Info", "Stage Name"),
      getValue [Long] (jsonData, "Stage Info", "Number of Tasks"),
      Array.fill (getValue [Long] (jsonData, "Stage Info", "Number of Tasks").toInt) (Task.empty),
      getValue [Seq[BigInt]] (jsonData, "Stage Info", "Parent IDs").map (_.toLong).toArray,
      getJValue (jsonData, "Stage Info", "RDD Info").children.map (new RDD(_)).toArray
      )
  }

  def addTask(task: Task) = {
    tasks(task.idx.toInt) = task
  }

  def firstRDD: RDD = {
    rdds.minBy (_.id)
  }
  
  def adaptiveRDD: RDD = {
    val adaptables = rdds.filter (_.isAdaptable)
    val maxScope = adaptables.map(_.scope).max
    adaptables.filter(_.scope == maxScope).minBy (_.id)
  }

  def emptyTasks: Array[Task] = tasks.filter (_.recordsRead == 0)

  def adaptivePoint: String = {
    val frdd = adaptiveRDD
    if (frdd.name contains "adaptive")
      frdd.name
    else
      "-"
  }

  def canAdapt: Boolean =
    !onlyAdaptive || (adaptivePoint contains "adaptive")

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
}
