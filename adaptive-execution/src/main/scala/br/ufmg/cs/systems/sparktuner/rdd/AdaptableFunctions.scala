package br.ufmg.cs.systems.sparktuner.rdd

import br.ufmg.cs.systems.common.Logging
import br.ufmg.cs.systems.sparktuner.OptHelper
import br.ufmg.cs.systems.sparktuner.Analyzer.PolicyFunc
import br.ufmg.cs.systems.util.PrivateMethodExposer.p

import org.apache.spark.rdd._

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.PairRDDFunctions

import org.apache.spark.graphx._

import java.lang.StackTraceElement

object AdaptableFunctions extends Logging {

  def preAdapt(conf: SparkConf, extraPolicies: (String,PolicyFunc)*) = {
    val start = System.currentTimeMillis
    val optHelper = OptHelper.get (conf, extraPolicies)
    val finish = System.currentTimeMillis
    logInfo (s"PreAdapt executed, result = ${optHelper}; elapsed: ${finish - start} ms")
    optHelper
  }

  implicit def rdd2adaptableFunctions [K : ClassTag, V: ClassTag]
    (rdd: RDD[(K,V)]) = new AdaptableFunctionsKv(rdd)

  implicit def scAdaptableFuntions (sc: SparkContext) = 
    new AdaptableFunctionsSc(sc)

  implicit def adaptableGraph [VD: ClassTag, ED: ClassTag]
    (graph: Graph[VD,ED]) = new AdaptableGraph(graph)
  
  implicit def adaptableVertexRdd [VD: ClassTag]
    (vrdd: VertexRDD[VD]) = new AdaptableVertexRDD(vrdd)

}

class AdaptableGraph[VD: ClassTag, ED: ClassTag] (graph: Graph[VD,ED]) {
  import AdaptableFunctions.rdd2adaptableFunctions
    
  println (s"%%%%%%%%%% Adaptable Graph Created !!!")

  def apply = {
    import AdaptableFunctions.rdd2adaptableFunctions
  }

  def vertices: VertexRDD[VD] = {
    println (s"%%%%%% Calling adptable vertices")
    graph.vertices
  }
  
  def edges: EdgeRDD[ED] = {
    println (s"%%%%%% Calling adptable edges")
    graph.edges
  }

}

class AdaptableVertexRDD[VD: ClassTag] (vrdd: VertexRDD[VD]) {
  import AdaptableFunctions.rdd2adaptableFunctions

  def apply = {
    import AdaptableFunctions.rdd2adaptableFunctions
    println (s"%%%%%%%%%% Adaptable VertexRDD Created !!!")
  }
}

sealed trait AdaptableFunctions extends Logging with Serializable {

  def optHelper: OptHelper
  
  def getAdptName[T](_adptName: String): String = Option(_adptName) match {
    case Some(adptName) => adptName
    case None =>
      null
      //val stack = Thread.currentThread.getStackTrace()(1)
      //s"${stack.getMethodName}:${stack.getLineNumber}"
  }

  def setAPName[T](rdd: RDD[T], _adptName: String): RDD[T] = {
    val adptName = if (_adptName != null) _adptName else rdd.name

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
  
  override def optHelper: OptHelper = OptHelper.get (sc.getConf)

  def textFile(path: String,
      minPartitions: Int = sc.defaultMinPartitions): RDD[String] = {
    textFile (path, minPartitions, null)
  }

  def textFile(
      path: String,
      minPartitions: Int,
      _adptName: String): RDD[String] = {
    val adptName = getAdptName (_adptName)
    setAPName (sc.textFile (path, optHelper.getNumPartitions(minPartitions, adptName)),
      adptName)
  }
}

class AdaptableFunctionsKv [K,V] (self: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctions[K,V](self) with AdaptableFunctions {

  def keyTag = kt

  def valueTag = vt

  def keyOrd = ord

  override def optHelper: OptHelper = OptHelper.get (self.sparkContext.getConf)
 
  /** override default functions to force adaptative behavior **/

  override def partitionBy(partitioner: Partitioner): RDD[(K, V)] = {
    partitionBy (partitioner, null)
  }

  override def reduceByKey (func: (V, V) => V): RDD[(K, V)] =
    reduceByKey (func, null)

  override def reduceByKey (func: (V, V) => V, numPartitions: Int): RDD[(K,V)] =
    reduceByKey (func, numPartitions, null)
  
  override def join [W] (other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] = {
    join (other, numPartitions, null)
  }

  /******/

  def reduceByKey (func: (V, V) => V, _adptName: String): RDD[(K, V)] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self)
    setAPName (self.reduceByKey(partitioner, func), adptName)
  }

  def reduceByKey (func: (V, V) => V, numPartitions: Int,
      _adptName: String): RDD[(K, V)] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self,
      defaultNumPartitions = numPartitions)
    setAPName (self.reduceByKey(partitioner, func), adptName)
  }

  def aggregateByKey [U: ClassTag] (zeroValue: U, numPartitions: Int, _adptName: String)(
      seqOp: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self,
      defaultNumPartitions = numPartitions)
    setAPName (self.aggregateByKey (zeroValue, partitioner)(seqOp, combOp), adptName)
  }

  def partitionBy(partitioner: Partitioner, _adptName: String): RDD[(K, V)] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self)
    setAPName (self.partitionBy (partitioner), adptName)
  }

  def join [W] (other: RDD[(K, W)], numPartitions: Int,
      _adptName: String): RDD[(K, (V, W))] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self,
      defaultNumPartitions = numPartitions)
    setAPName (self.join (other, partitioner), adptName)
  }

  def join [W] (other: RDD[(K, W)], _adptName: String): RDD[(K, (V, W))] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self)
    setAPName (self.join (other, partitioner), adptName)
  }

}
