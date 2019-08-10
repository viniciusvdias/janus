package br.ufmg.cs.systems.sparktuner.rdd

import br.ufmg.cs.systems.common.Logging
import br.ufmg.cs.systems.sparktuner.{JanusPartitioner, OptHelper}
import br.ufmg.cs.systems.util.PrivateMethodExposer.p

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.aspectj.lang._
import org.aspectj.lang.annotation._

import org.apache.spark._
import org.apache.spark.rdd._

@Aspect
class RDDInterceptor extends Logging {

  def exclusionFunction(className: String): Boolean = {
    // A regular expression to match classes of the internal Spark API's
    // that we want to skip when finding the call site of a method.
    val SPARK_CORE_CLASS_REGEX =
      """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]""".r
    val SPARK_SQL_CLASS_REGEX = """^org\.apache\.spark\.sql.*""".r
    val SCALA_CORE_CLASS_PREFIX = "scala"
    val JANUS_CLASS_PREFIX = "br.ufmg.cs.systems.sparktuner"
    val AJ_CLASS_PREFIX = "org.aspectj"
    val isSparkClass = SPARK_CORE_CLASS_REGEX.findFirstIn(className).isDefined ||
      SPARK_SQL_CLASS_REGEX.findFirstIn(className).isDefined
    val isScalaClass = className.startsWith(SCALA_CORE_CLASS_PREFIX)
    val isJanusClass = className.startsWith(JANUS_CLASS_PREFIX)
    val isAjClass = className.startsWith(AJ_CLASS_PREFIX)
    // If the class is a Spark internal class or a Scala class, then exclude.
    isSparkClass || isScalaClass || isJanusClass || isAjClass
  }
  
  def getCallSite(skipClass: String => Boolean = exclusionFunction): (String, String) = {
    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var insideSpark = true
    val callStack = new ArrayBuffer[String]() :+ "<unknown>"

    Thread.currentThread.getStackTrace().foreach { ste: StackTraceElement =>
      // When running under some profilers, the current stack trace might contain some bogus
      // frames. This is intended to ensure that we don't crash in these situations by
      // ignoring any frames that we can't examine.
      if (ste != null && ste.getMethodName != null
        && !ste.getMethodName.contains("getStackTrace")) {
        if (insideSpark) {
          if (skipClass(ste.getClassName)) {
            lastSparkMethod = if (ste.getMethodName == "<init>") {
              // Spark method is a constructor; get its class name
              ste.getClassName.substring(ste.getClassName.lastIndexOf('.') + 1)
            } else {
              ste.getMethodName
            }
            callStack(0) = ste.toString // Put last Spark method on top of the stack trace.
          } else {
            if (ste.getFileName != null) {
              firstUserFile = ste.getFileName
              if (ste.getLineNumber >= 0) {
                firstUserLine = ste.getLineNumber
              }
            }
            callStack += ste.toString
            insideSpark = false
          }
        } else {
          callStack += ste.toString
        }
      }
    }

    val callStackDepth = System.getProperty("spark.callstack.depth", "20").toInt
    val shortForm =
      if (firstUserFile == "HiveSessionImpl.java") {
        // To be more user friendly, show a nicer string for queries submitted from the JDBC
        // server.
        "Spark JDBC Server Query"
      } else {
        s"$lastSparkMethod at $firstUserFile:$firstUserLine"
      }
    val longForm = callStack.take(callStackDepth).mkString("\n")

    (shortForm, longForm)
  }

  @After("execution(org.apache.spark.rdd.RDD.new(..))")
  def rddConstructor(joinPoint: JoinPoint): Unit = {
    val rdd = joinPoint.getThis().asInstanceOf[RDD[_]]
    val className = rdd.getClass.getSimpleName
    // val creationSite = p(rdd)('getCreationSite)().asInstanceOf[String]
    val creationSite = getCallSite()._1
    val name = s"${className}-${creationSite}"
    logInfo (s"Setting ${rdd} name to ${name}")
    rdd.setName(name)
  }

  def getAPName(rdd: RDD[_]): String = {
    @scala.annotation.tailrec
    def getAPNameRec(rdd: RDD[_]): String = rdd.dependencies match {
      case deps if deps.isEmpty =>
        rdd.name
      case deps if deps.head.isInstanceOf[ShuffleDependency[_,_,_]] =>
        rdd.name
      case deps =>
        getAPNameRec(deps.head.rdd)
    }

    getAPNameRec(rdd)
  }

  private def getTagsAndOrdering[K,V](functions: PairRDDFunctions[K,V])
    : (ClassTag[K], ClassTag[V], Ordering[K]) = {
    (ClassTag[K](p(functions)('keyClass)().asInstanceOf[Class[_]]),
      ClassTag[V](p(functions)('valueClass)().asInstanceOf[Class[_]]),
      p(functions)('keyOrdering)().asInstanceOf[Option[Ordering[K]]].
        getOrElse(null))
  }

  @Around(
    "execution(* org.apache.spark.rdd.PairRDDFunctions.groupByKey(..))")
  def vrddGroupByKey1(joinPoint: ProceedingJoinPoint): Any = {
    val args = joinPoint.getArgs()
    val targetRdd = joinPoint.proceed(args).asInstanceOf[RDD[_]]
    targetRdd.partitioner match {
      case Some(p : JanusPartitioner) =>
        return targetRdd
      case _ =>
    }
    
    val adptName = getAPName(targetRdd)
    
    def customGroupByKey [K,V,W] (functions: PairRDDFunctions[K,V]): Any = {
      val rdd1 = targetRdd.dependencies.head.rdd.asInstanceOf[RDD[(K,V)]]
      val optHelper = OptHelper.get (rdd1.sparkContext.getConf)

      // get implicits from functions
      implicit val (ct, vt, ord) = getTagsAndOrdering(functions)

      val partitioner = optHelper.getPartitioner (adptName, prev = rdd1)
      val adptRDD = functions.groupByKey (partitioner)

      logInfo (s"InterceptGroupByKey adptName=${adptName}" +
        s" rdd1=${rdd1}" +
        s" targetRdd=${targetRdd} adptRDD=${adptRDD}")

      adptRDD
    }

    val functions = joinPoint.getThis().asInstanceOf[PairRDDFunctions[_,_]]
    customGroupByKey(functions)
  }

  @Around(
    "execution(* org.apache.spark.rdd.PairRDDFunctions.join(org.apache.spark.rdd.RDD))")
  def vrddJoin1(joinPoint: ProceedingJoinPoint): Any = {
    val args = joinPoint.getArgs()
    val targetRdd = joinPoint.proceed(args).asInstanceOf[RDD[_]]
    targetRdd.partitioner match {
      case Some(p : JanusPartitioner) =>
        return targetRdd
      case _ =>
    }

    val adptName = getAPName(targetRdd)
    
    def customJoin [K,V,W] (functions: PairRDDFunctions[K,V]): Any = {
      val rdd1 = targetRdd.dependencies.head.rdd.asInstanceOf[RDD[(K,V)]]
      val rdd2 = joinPoint.getArgs()(0).asInstanceOf[RDD[(K,W)]]
      val optHelper = OptHelper.get (rdd2.sparkContext.getConf)

      // get implicits from functions
      implicit val (ct, vt, ord) = getTagsAndOrdering(functions)

      val partitioner = optHelper.getPartitioner (adptName, prev = rdd1)
      val adptRDD = functions.join (rdd2, partitioner)

      logInfo (s"InterceptJoin adptName=${adptName}" +
        s" rdd1=${rdd1} rdd2=${rdd2}" +
        s" targetRdd=${targetRdd} adptRDD=${adptRDD}")

      adptRDD
    }

    val functions = joinPoint.getThis().asInstanceOf[PairRDDFunctions[_,_]]
    customJoin(functions)
  }
}
