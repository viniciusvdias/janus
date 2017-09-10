import sbt._
import Keys._

object SparkExtensionsBuild extends Build {
    
  crossScalaVersions in ThisBuild := Seq("2.10.4", "2.11.8")
  libraryDependencies in ThisBuild ++= Seq(
    "org.scalatest" %% "scalatest_2.11" % "2.2.1" % "test"
  )

  lazy val commonSettings = Seq(
    organization := "br.ufmg.cs",
    version := "0.1.0",
    crossScalaVersions := Seq("2.10.4", "2.11.8")
  )
  
  lazy val root = Project(id = "spark-extensions", base = file(".")).
    settings (commonSettings: _*).
    aggregate (common, adaptive, algorithms)

  lazy val common = Project(id = "common",
    base = file("common")).
    settings (commonSettings: _*)

  lazy val adaptive = Project(id = "adaptive-execution",
    base = file("adaptive-execution")).
    settings (commonSettings: _*).
    dependsOn(common)

  lazy val algorithms = Project(id = "algorithms",
    base = file("algorithms")).
    settings (commonSettings: _*).
    dependsOn(adaptive, common)
}
