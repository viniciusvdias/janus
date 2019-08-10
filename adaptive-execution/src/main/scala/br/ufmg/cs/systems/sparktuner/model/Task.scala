package br.ufmg.cs.systems.sparktuner.model

import br.ufmg.cs.systems.sparktuner.Analyzer._

import org.json4s._
import org.json4s.native.JsonMethods._

/**
 * Instances of this class hold information about the execution of one task
 * of a Spark application. Every task is associated with one unique stage and
 * refers to the computation of one RDD partition.
 * Base execution statistics are extracted from here.
 */
class Task(
    // general
    val stageId: Long,              // identifies the stage of this task
    val idx: Long,                  // task ID within the stage (0 until numPartitions - 1)
    val executorId: String,         // identifies the executor that ran this task
    val host: String,               // identifies the host that has the executor that ran this task
    val locality: String,               // identifies the host that has the executor that ran this task
    val runTime: Long,              // total execution time
    val gcTime: Long,               // time in which the executor was GCing while running this task
    val inputRecords: Long,         // number of input records (hdfs or caching)
    val inputBytes: Long,           // size of input in bytes (hdfs or caching)
    // spill
    val memoryBytesSpilled: Long,   // size of memory spill in bytes
    val diskBytesSpilled: Long,     // size of disk spill in bytes
    // shuffles
    val shuffleReadRecords: Long,   // number of records read by the shuffle
    val shuffleReadBytes: Long,     // size of the shuffle read in bytes
    val fetchWaitTime: Long,        // idle time waiting for data coming through network (shuffle)
    val shuffleWriteRecords: Long,  // number of records written by the shuffle
    val shuffleWriteBytes: Long     // size of the shuffle write in bytes
  ) {

  def this(taskData: JValue) {
    this (
      getValue [Long] (taskData, "Stage ID"),
      getValue [Long] (taskData, "Task Info", "Index"),
      getValue [String] (taskData, "Task Info", "Executor ID"),
      getValue [String] (taskData, "Task Info", "Host"),
      getValue [String] (taskData, "Task Info", "Locality"),
      getValue [Long] (taskData, "Task Metrics", "Executor Run Time"),
      getValue [Long] (taskData, "Task Metrics", "JVM GC Time"),
      getValue [Long] (taskData, "Task Metrics", "Input Metrics", "Records Read"),
      getValue [Long] (taskData, "Task Metrics", "Input Metrics", "Bytes Read"),
      getValue [Long] (taskData, "Task Metrics", "Memory Bytes Spilled"),
      getValue [Long] (taskData, "Task Metrics", "Disk Bytes Spilled"),
      getValue [Long] (taskData, "Task Metrics", "Shuffle Read Metrics", "Total Records Read"),
      getValue [Long] (taskData, "Task Metrics", "Shuffle Read Metrics", "Local Bytes Read") +
        getValue [Long] (taskData, "Task Metrics", "Shuffle Read Metrics", "Remote Bytes Read"),
      getValue [Long] (taskData, "Task Metrics", "Shuffle Read Metrics", "Fetch Wait Time"),
      getValue [Long] (taskData, "Task Metrics", "Shuffle Write Metrics", "Shuffle Records Written"),
      getValue [Long] (taskData, "Task Metrics", "Shuffle Write Metrics", "Shuffle Bytes Written")
      )
  }

  def recordsRead = inputRecords + shuffleReadRecords
  
  def recordsReadBytes = inputBytes + shuffleReadBytes

  def bytesSpilled = memoryBytesSpilled + diskBytesSpilled

  // smaller the better
  def gcOverhead: Double = gcTime / runTime.toDouble
  
  def blockedReadOverhead: Double = fetchWaitTime / runTime.toDouble

  override def toString = {
    s"Task(idx=${idx},runtime=${runTime},gcTime=${gcTime},inputRecords=${inputRecords}" +
    s",shuffleReadRecords=${shuffleReadRecords},shuffleReadBytes=${shuffleReadBytes}" +
    s",shuffleWriteRecords=${shuffleWriteRecords},shuffleWriteBytes=${shuffleWriteBytes}" +
    ")"
  }
}

object Task {
  lazy val empty: Task = new Task(0,0,null,null,null,0,0,0,0,0,0,0,0,0,0,0)
}
