package br.ufmg.cs.systems.sparktuner.rdd

import br.ufmg.cs.systems.common.Logging
import br.ufmg.cs.systems.sparktuner.OptHelper
import br.ufmg.cs.systems.sparktuner.Analyzer.PolicyFunc
import br.ufmg.cs.systems.util.PrivateMethodExposer.p

import org.apache.spark.rdd._

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.PairRDDFunctions

import java.lang.StackTraceElement

object AdaptableFunctions extends Logging {

  def preAdapt(sc: SparkContext, extraPolicies: (String,PolicyFunc)*) = {
    val start = System.currentTimeMillis
    val optHelper = OptHelper.get (sc, extraPolicies)
    val finish = System.currentTimeMillis
    logInfo (s"PreAdapt executed, result = ${optHelper}; elapsed: ${finish - start} ms")
    optHelper
  }

  implicit def rdd2adaptableFunctions [K : ClassTag, V: ClassTag]
    (rdd: RDD[(K,V)]) = new AdaptableFunctionsKv(rdd)

  implicit def scAdaptableFuntions (sc: SparkContext) = 
    new AdaptableFunctionsSc(sc)

}

sealed trait AdaptableFunctions extends Logging with Serializable {

  def optHelper: OptHelper
  
  def getAdptName[T](_adptName: String): String = Option(_adptName) match {
    case Some(adptName) => adptName
    case None =>
      val stack = Thread.currentThread.getStackTrace()(1)
      s"${stack.getMethodName}:${stack.getLineNumber}"
  }

  def setAPName[T](rdd: RDD[T], adptName: String): RDD[T] = {
    @scala.annotation.tailrec
    def setAPNameRec(rdd: RDD[_]): Unit = rdd.dependencies.head match {
      case shufDep: ShuffleDependency[_,_,_] =>
        rdd.setName (adptName)
      case dep if !dep.rdd.dependencies.isEmpty =>
        setAPNameRec (dep.rdd)
      case dep =>
        dep.rdd.setName (adptName)
    }
    setAPNameRec (rdd)
    rdd
  }
}

class AdaptableFunctionsSc (sc: SparkContext) extends AdaptableFunctions {
  
  override def optHelper: OptHelper = OptHelper.get (sc)

  def textFile(path: String,
      minPartitions: Int = sc.defaultMinPartitions): RDD[String] = {
    textFile (path, minPartitions, null)
  }

  def textFile(
      path: String,
      minPartitions: Int,
      _adptName: String): RDD[String] = {
    implicit val altAdptName = s"${path}-textFile"
    val adptName = getAdptName (_adptName)
    setAPName (sc.textFile (path, optHelper.getNumPartitions(minPartitions, adptName)),
      adptName)
  }
}

class AdaptableFunctionsKv [K,V] (self: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctions[K,V](self) with AdaptableFunctions {

  override def optHelper: OptHelper = OptHelper.get (self.sparkContext)
 
  /** override default functions to force adaptative behavior **/

  override def reduceByKey (func: (V, V) => V): RDD[(K, V)] =
    reduceByKey (func, null)

  override def reduceByKey (func: (V, V) => V, numPartitions: Int): RDD[(K,V)] =
    reduceByKey (func, numPartitions, null)
  
  override def join [W] (other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] = {
    join (other, numPartitions, null)
  }

  /******/

  def reduceByKey (func: (V, V) => V, _adptName: String): RDD[(K, V)] = {
    implicit val altAdptName = "ShuffledRDD-reduceByKey"
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self)
    setAPName (self.reduceByKey(partitioner, func), adptName)
  }

  def reduceByKey (func: (V, V) => V, numPartitions: Int,
      _adptName: String): RDD[(K, V)] = {
    implicit val altAdptName = "ShuffledRDD-reduceByKey"
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self,
      defaultNumPartitions = numPartitions)
    setAPName (self.reduceByKey(partitioner, func), adptName)
  }

  def aggregateByKey [U: ClassTag] (zeroValue: U, numPartitions: Int, _adptName: String)(
      seqOp: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)] = {
    implicit val altAdptName = "ShuffledRDD-aggregateByKey"
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self,
      defaultNumPartitions = numPartitions)
    setAPName (self.aggregateByKey (zeroValue, partitioner)(seqOp, combOp), adptName)
  }

  def partitionBy(partitioner: Partitioner, _adptName: String): RDD[(K, V)] = {
    implicit val altAdptName = "ShuffledRDD-partitionBy"
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self)
    setAPName (self.partitionBy (partitioner), adptName)
  }

  def join [W] (other: RDD[(K, W)], numPartitions: Int,
      _adptName: String): RDD[(K, (V, W))] = {
    implicit val altAdptName = "CoGroupedRDD-join"
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self,
      defaultNumPartitions = numPartitions)
    setAPName (self.join (other, partitioner), adptName)
  }

  def join [W] (other: RDD[(K, W)], _adptName: String): RDD[(K, (V, W))] = {
    implicit val altAdptName = "CoGroupedRDD-join"
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self)
    setAPName (self.join (other, partitioner), adptName)
  }

}
