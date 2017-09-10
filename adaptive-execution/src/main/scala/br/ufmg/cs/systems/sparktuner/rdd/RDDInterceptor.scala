package br.ufmg.cs.systems.sparktuner.rdd

import br.ufmg.cs.systems.common.Logging

import org.aspectj.lang._
import org.aspectj.lang.annotation._

import org.apache.spark._
import org.apache.spark.rdd._

@Aspect
class RDDInterceptor extends Logging {

  @Around("execution(* org.apache.spark.rdd.RDD+.getPartitions(..))")
  def rddGetPartitions(joinPoint: ProceedingJoinPoint): Any = {
    val partitions = joinPoint.proceed().asInstanceOf[Array[Partition]]
    
    logInfo (s"Getting ${partitions.length} partitions.")

    partitions
  }

  @Around(
    "execution(* org.apache.spark.rdd.PairRDDFunctions.partitionBy(org.apache.spark.Partitioner))")
  def vrddPartitionBy(joinPoint: ProceedingJoinPoint): Any = {
    val args = joinPoint.getArgs()

    val reqPartitioner = args(0).asInstanceOf[Partitioner]
    val adptPartitioner = new HashPartitioner(11)

    logInfo (s"Requested partitioner ${reqPartitioner}" +
      s" numPartitions=${reqPartitioner.numPartitions}")
    logInfo (s"Adaptive partitioner ${adptPartitioner}" +
      s" numPartitions=${adptPartitioner.numPartitions}")

    args(0) = adptPartitioner
    joinPoint.proceed(args)
  }

}
