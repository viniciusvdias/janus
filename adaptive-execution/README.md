
Adaptive Execution
==================
Reconfiguration tool for offline adaptation of recurrent applications.
Also it provides an API for writing custom policies.

## Overview
The tool operates around the concept of `adaptive point`. This definition makes a reference to a point in a Spark application
(more specifically, its DAG) in which we have the opportunity to change the default degree of parallelism. Most of these points are
obtained by applying an operator that requires shuffle to an RDD, like `reduceByKey`, `groupByKey`,
`repartition`, etc. Such operators already expose an user API that supports the partitioning customization by *(1)* changing the derived number of partitions or *(2)* setting up a different partitioning strategy, i.e., the *partitioner*. 

## Analyzer
The purpose of the `Analyzer` is to receive execution logs, identify points for adaptation in the
application, apply the configured policies based on the data
collected and finally deliver a set of actions to the `Adaptation Helper`.

### Optimization Policies

User can define custom policies by writing a scala method with the following signature:

```scala
def myPolicy(rdd: RDD, stages: List[Stage]): Action
```

After that the new policy can be included in the system through the call `preAdapt`:

```
preAdapt(sc, p(1), p(2), ...)
```

where `sc` is the SparkContext and `p(i)` is the custom policy name, i.e., the defined function name (*myPolicy*, for example). Thus, just call it right after the SparkContext definition and you will be all set with your policy.

### Actions

- `UNPAction(adaptivePointName, numPartitions)`: It **U**pdates the **N**umber of **P**artitions of an adaptive point.
- `UPAction(adaptivePointName, partitionerName)`: It **U**pdates the **P**artitioner of an adaptive point.
- `WarnAction(adaptivePointName, messageString)`: It provides means to the tool user trigger warnings when some policy is applied.

## Adaptation Helper
The `Adaptation Helper` translates actions delivered by the `Analyzer` into real modifications in the current execution, transparently to the user.

### Tool API
The tool is able to change the default behavior of operators by overriding them at runtime with Scala implicits. This is totally transparent to the user. In order to accomplish this the user must import a set of adaptable functions at the beggining of its implementation. Naturally we support for now only the Spark Scala API. The following line of import must be added to your code:

```scala
import br.ufmg.cs.systems.sparktuner.rdd.AdaptableFunctions._
```

A list of common supported operators: *textFile, reduceByKey, aggregateByKey, join and partitionBy*

## Tool behavior
As described the tool behavior can be separated in two main steps, given that the user has imported the overwritten operators and has specified a previous log information. In the first step the system must retrieve the log, parse it and decide actions from it. The actual moment that this happens


## Wordcount Example
The following code emphasizes the differences of an implementation that supports the tool.
The application is *Wordcount*, optional or mandatory differences are commented as such.

```scala
import br.ufmg.cs.systems.sparktuner.rdd.AdaptableFunctions._ // mandatory modification

object Wordcount {
  def myPolicy(rdd: RDD, stages: List[Stage]): Action = {
    // use stages to extract statistics about the performance of this adaptive point
    // return one of the supported actions
  }

  def main(args: Array[String]) {
  
    val conf = new SparkConf().setAppName("Word Count").
      set("spark.adaptive.logpath", "/tmp/log1") // mandatory modification
    val sc = new SparkContext(conf)
    preAdapt(sc, myPolicy) // optional modification, 
    
    val counts = sc.textFile ("/tmp/sample").
      flatMap (_ split ).
      map (w => (w,1)).
      reduceByKey (_ + _, "ap-counting") // optional modification, we consider a default adaptive name
      
    println (counts.count + "words")
    
    sc.stop()
  }
}
```

Some algorithms in [Algorithms Project](https://github.com/viniciusvdias/spark-extensions/tree/master/algorithms) have been extended to support the tool.

## Testing the included algorithms

The following steps assume that Spark is properly installed.

1. Clone the repository

```
git clone https://github.com/viniciusvdias/spark-adaptive-exec
```

2. Build all projects

```
cd spark-adaptive-exec && sbt clean assembly
```

3. Specify a sample log and run the Eclat algorithm. Note that we can instead pass the log configuration by command line:
```
spark-submit --master local --class br.ufmg.cs.lib.dmining.fim.eclat.Eclat \
	--conf spark.adaptive.logpath=algorithms/scripts/analyzer/eclat.log \
    algorithms/target/scala-2.11/spark-algorithms-assembly-1.0.jar \
    --inputFile algorithms/input/twig-input.txt --minSupport 0.1
```
