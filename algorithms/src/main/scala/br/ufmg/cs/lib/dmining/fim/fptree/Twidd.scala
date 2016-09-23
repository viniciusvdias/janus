/* Twidd.scala */
package br.ufmg.cs.lib.dmining.fim.fptree

// command line parsing
import br.ufmg.cs.util.ParamsParser

// common
import br.ufmg.cs.util.Common
import br.ufmg.cs.systems.sparktuner.rdd.AdaptableFunctions._

import org.apache.log4j.{Logger, Level}

// spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.{Partitioner, HashPartitioner, RangePartitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.AccumulatorParam
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.{ClassTag, classTag}

// scala stuff
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray
import scala.collection.mutable.Map
import scala.util.Sorting
import scala.math.Ordering
import scala.math.Ordered

import scala.reflect._
import org.apache.spark.util.SizeEstimator

object Twidd {
  
  /**
   * @param inputFile path to file containing transactions
   * @param minSupport minimum percentage of support
   * @param numPartitions 3 numbers representing the requested partitioning in
   * three points of the algorithm, respectively:
   *  - input
   *  - mu repartition
   *  - rho repartition
   * @param mu balancing parameter (Twig)
   * @param rho pre-projection: balancing parameter (Twig)
   * @param sep items delimiter
   * @param logLevel log4j log level
   */
  case class Params(inputFile: String = null,
      minSupport: Double = 0.5,
      numPartitions: Array[Int] = Array (128,128,128,128),
      mu: Int = 2,
      rho: Int = 2,
      sep: String = "\\s+",
      logLevel: Level = Level.OFF) {

    def getValues = (inputFile, minSupport, numPartitions,
      mu, rho, sep, logLevel)

    override def toString =
      "&%@ params " + getValues.productIterator.mkString (" ")
  }

  val appName = "FP-Twidd"
  val log = Logger.getLogger(appName)

  type ItemSet = WrappedArray[Int]
  def itemSetCompare (arr1: ItemSet, arr2: ItemSet): Boolean = {
    if (arr1.size < arr2.size) return true
    else if (arr2.size < arr1.size) return false

    var i = 0
    while (i < arr1.size && i < arr2.size) {
      if (arr1(i) < arr2(i)) return true
      else if (arr2(i) < arr1(i)) return false
    }
    return false
  }
  val itemSetOrd = Ordering.fromLessThan[ItemSet] (itemSetCompare)

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new ParamsParser[Params](appName)
    parser.opt ("inputFile",
      s"path to transactions input file, default: ${defaultParams.inputFile}",
      (value,tmpParams) => tmpParams.copy (inputFile = value),
      true
      )

    parser.opt ("minSupport",
      s"percentage for considering itemsets frequent, default: ${defaultParams.minSupport}",
      (value,tmpParams) => tmpParams.copy (minSupport = value.toDouble)
      )

    parser.opt ("numPartitions",
      s"sequence of number of partitions, default: ${defaultParams.numPartitions.mkString("[",",","]")}",
      (value,tmpParams) => tmpParams.copy (numPartitions = value.split(",").map (_.toInt))
      )
    
    parser.opt ("mu",
      s"mu load balacing factor, default: ${defaultParams.mu}",
      (value,tmpParams) => tmpParams.copy (mu = value.toInt)
      )

    parser.opt ("rho",
      s"rho pre-projection factor, default: ${defaultParams.rho}",
      (value,tmpParams) => tmpParams.copy (rho = value.toInt)
      )

    parser.opt ("separator",
      s"item separator, default: ${defaultParams.sep}",
      (value,tmpParams) => tmpParams.copy (sep = value)
      )

    parser.opt ("logLevel",
      s"log4j level, default: ${defaultParams.logLevel}",
      (value,tmpParams) => tmpParams.copy (logLevel = Level.toLevel(value))
      )
    
    parser.parse (args, defaultParams) match {
      case Some(params) => run(params)
      case None => sys.exit(1)
    }
  }

  /*
   * create transactions RDD from raw lines.
   */
  def getTransactions (linesRDD: RDD[String], sep: String) = {
    
    val transactionsRDD = linesRDD
      .map {l =>
        (l split sep).flatMap {str =>
          // ignore non-32-Integers
          try Iterator(str.toInt)
          catch {
            case e: java.lang.NumberFormatException =>
              Iterator.empty
          }
        }
      }
      .persist (StorageLevel.MEMORY_ONLY_SER)
      .setName ("transactions_rdd")

    transactionsRDD
  }

  /*
   * each RDD partition turns into a local tree.
   */
  def localTrees(
    transactionsRDD: RDD[Array[Int]],
    freqsBc: Broadcast[scala.collection.Map[Int,Int]]) = {

    // building *numPartitions* local trees
    val localTreesRDD = transactionsRDD
      .map (_.filter(freqsBc.value.contains(_))
        .sortWith {(it1, it2) =>
          val cmp = (freqsBc.value(it1) compare freqsBc.value(it2))
          if (cmp == 0) it1 < it2
          else cmp > 0
        }
      )
      .mapPartitions {transIter =>
        val tree = FPTree()
        tree.buildTree(transIter)
        Iterator(tree)
      }
    localTreesRDD
  }

  /*
   * extract mu-trees (prefix,subtree) from local trees.
   */
  def muTrees(
      localTreesRDD: RDD[FPTree],
      mu: Int) = {

      val muTreesRDD = localTreesRDD.flatMap {t =>
        val _mu = mu
        FPTree.muTrees(t, _mu)
    }

    muTreesRDD
  }

  /*
   * shuffle mu-trees and merge them into balanced fp trees.
   */
  def mergeMuTrees(
      muTreesRDD: RDD[(ItemSet,Any)],
      partitioner: Partitioner) = {

    val fpTreesRDD = muTreesRDD.
      partitionBy (partitioner, "adaptive-point-mergedmutrees").
      mapPartitions ({
        case muTreesIter if !muTreesIter.isEmpty =>
          val tree = FPTree()
          while (!muTreesIter.isEmpty) {
            val (p,t) = muTreesIter.next
            tree.mergeMuTree(p, t)
          }
          Iterator(tree)
        case _ => Iterator.empty
      }, true)

    fpTreesRDD
  }

  /*
   * pre-projection based on rho factor.
   */
  def rhoTrees(
      fpTreesRDD: RDD[FPTree],
      rho: Int) = {

    val rhoTreesRDD = fpTreesRDD.flatMap {FPTree.rhoTrees(_,rho)}
    rhoTreesRDD

  }

  /*
   * shuffle pre-projected rho-trees and merge them into final fp trees.
   */
  def mergeRhoTrees(
      rhoTreesRDD: RDD[(ItemSet,Any)],
      partitioner: Partitioner) = {

    val finalFpTreesRDD = rhoTreesRDD.reduceByKey (partitioner,
      (k,v) => (k,v) match {
        case (t1:Int,t2:Int) => t1 + t2
        case (t1:FPTree,t2:FPTree) => t1.mergeRhoTree(t2); t1
      })

    finalFpTreesRDD
  }

  /*
   * finish projection by running fpGrowth on each final tree.
   */
  def runFPGrowth(
      finalFpTreesRDD: RDD[(ItemSet,Any)],
      minCount: Int) = {

    val itemSetsRDD = finalFpTreesRDD.flatMap {
      case (p,sup:Int) =>
        if (sup > minCount) Iterator( (p.toArray, sup) ) else Iterator.empty
      case (p,t:FPTree) =>
        t.itemSet = p.toArray; FPTree.fpGrowth(t, minCount)
    }

    itemSetsRDD
  }

  /* 
   * custom partitioner: it guarantees that subtrees with the same prefix are
   * grouped together.
   */
  class TreePartitioner(underlyingPartitioner: Partitioner) extends Partitioner {

    def this (nparts: Int) = {
      this (new HashPartitioner (nparts))
    }

    def numPartitions: Int = underlyingPartitioner.numPartitions

    def getPartition(key: Any): Int = underlyingPartitioner.getPartition(key)

    override def equals(other: Any): Boolean = other.isInstanceOf[TreePartitioner]
  }

  def run(params: Params, confOpt: Option[SparkConf] = None) {

    // params as vals (lazy evaluation safety)
    val (inputFile, minSupport, numPartitions,
      mu, rho, sep, logLevel) = params.getValues

    val timestamp = System.currentTimeMillis

    log.setLevel(logLevel)
    Logger.getLogger("org").setLevel(logLevel)
    Logger.getLogger("akka").setLevel(logLevel)
    
    log.info (s"\n\n${params}\n")

    val conf = confOpt.getOrElse(new SparkConf().setAppName(appName))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "br.ufmg.cs.lib.dmining.fim.fptree.TreeOptRegistrator")
    val sc = new SparkContext(conf)
    preAdapt (sc)

    // Start execution time
    var t0 = System.nanoTime

    // frequency counting
    val linesRDD = sc.textFile(inputFile, numPartitions(0),
      "adaptive-point-input") // PARTITIONING POINT 1: Reading Input
    
    // collection of transactions
    val transactionsRDD = getTransactions (linesRDD, sep)
    
    val transCount = transactionsRDD.count
    log.info (s"number of transactions = ${transCount}")
    log.info (Common.storageInfo (transactionsRDD))

    // minSupport percentage to minCount
    val minCount = (minSupport * transCount).toInt
    log.info (s"support count = ${minCount} (${minSupport * 100}%)")

    // count 1-itemsets
    val frequencyRDD = transactionsRDD.
      flatMap (trans => trans.iterator zip Iterator.continually(1)).
      reduceByKey ((x:Int,y:Int) => x+y, numPartitions(1),
        "adaptive-point-1itemset-counting").
      filter (_._2 > minCount)

    // broadcast variables
    val freqs = frequencyRDD.collectAsMap
    val freqsBc = sc.broadcast(freqs)
    //transactionsRDD.unpersist()

    log.info (s"${freqsBc.value.size} frequent 1-itemsets ...")
    
    // construct localTrees
    val localTreesRDD = localTrees (transactionsRDD, freqsBc)

    // muTrees are constructed based on the custom partitioner 'TreePartitioner'
    val muTreesRDD = muTrees (localTreesRDD, mu)

    // merge muTrees based on prefix
    val fpTreesRDD = mergeMuTrees (muTreesRDD,
      new TreePartitioner (numPartitions(2)) // PARTITIONING POINT 2: muTrees repartitioning
    )

    // build first projection trees and final conditional trees, respectively
    val rhoTreesRDD = rhoTrees (fpTreesRDD, rho)

    val finalFpTreesRDD = mergeRhoTrees (rhoTreesRDD,
      new TreePartitioner (numPartitions(3)) // PARTITIONING POINT 3: rhoTrees pre-projection
    )
    
    // perform remaining projections in the partitioned trees
    val itemSetsRDD = runFPGrowth (finalFpTreesRDD, minCount)

    logLevel match {
      case Level.OFF =>
        itemSetsRDD.foreach {case (it,sup) => println(it.mkString(",") + "\t" + sup)}

      case _ =>
        val currTime = System.currentTimeMillis
        val sufix = "_%s_%s.fpgrowth".format(inputFile.split("/").last,numPartitions.mkString("_"))
        itemSetsRDD.map {case (it,sup) => it.sorted.mkString(",") + " " + sup}
          .saveAsTextFile (currTime + "_%s_%s_%s".format(minSupport,mu,rho) + sufix)
    }
    // End job execution time
    log.info ("All execution took " + (System.nanoTime - t0) / (1000000000.0) + " seconds")
    sc.stop()
  }
}
