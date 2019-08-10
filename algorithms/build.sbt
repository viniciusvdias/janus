name := "spark-algorithms"

version := "1.0"

crossScalaVersions := Seq("2.10.4", "2.11.8")

parallelExecution in Test := false

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.0" % "provided"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"
