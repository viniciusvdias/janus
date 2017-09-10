name := "adaptive-execution"

version := "1.0"

crossScalaVersions := Seq("2.10.4", "2.11.8")

resolvers += Resolver.sonatypeRepo("public")

parallelExecution in Test := false

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.0" % "provided"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0"

libraryDependencies += "org.aspectj" % "aspectjtools" % "1.8.10"

libraryDependencies += "org.aspectj" % "aspectjweaver" % "1.8.10"

libraryDependencies += "org.aspectj" % "aspectjrt" % "1.8.10"

javaOptions += "-javaagent:" + System.getProperty("user.home") + "/.ivy2/cache/org.aspectj/aspectjweaver/jars/aspectjweaver-1.8.10.jar"

fork := true
