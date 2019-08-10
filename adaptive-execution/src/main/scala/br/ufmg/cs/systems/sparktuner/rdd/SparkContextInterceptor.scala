package br.ufmg.cs.systems.sparktuner.rdd

// import br.ufmg.cs.systems.common.Logging

import org.aspectj.lang._
import org.aspectj.lang.annotation._

import org.apache.spark._

@Aspect
class SparkContextInterceptor {
  //@Around("call(org.apache.spark.SparkContext.new(org.apache.spark.SparkConf))")
  //def logSparkContext(joinPoint: ProceedingJoinPoint): SparkContext = {
  //  val args = joinPoint.getArgs()
  //  val conf = new SparkConf().
  //    setAppName("Simple Application modified !!!!").
  //    setMaster("local")
  //  args(0) = conf
  //  val start = System.currentTimeMillis
  //  val retVal = joinPoint.proceed(args)
  //  val time = System.currentTimeMillis - start

  //  val buffer = new StringBuffer
  //  buffer.append(joinPoint.getTarget());
  //  buffer.append(".");
  //  buffer.append(joinPoint.getSignature().getName());
  //  buffer.append("(");
  //  buffer.append(joinPoint.getArgs());
  //  buffer.append(")");
  //  buffer.append(" execution time: ");
  //  buffer.append(time);
  //  buffer.append(" ms");
  //  
  //  println(s"buffer ${buffer.toString}")
  //  println(s"retVal ${retVal}")

  //  return retVal.asInstanceOf[SparkContext]

  //}
}
