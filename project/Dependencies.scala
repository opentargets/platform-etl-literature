import sbt._

object Dependencies {

  lazy val aoyi = Seq(
    "com.lihaoyi" %% "pprint" % "0.6.0"
  )

  lazy val betterFiles = "com.github.pathikrit" %% "better-files-akka" % "3.9.1"

  lazy val configDeps = Seq(
    "org.yaml" % "snakeyaml" % "1.21",
    "com.github.pureconfig" %% "pureconfig" % "0.14.1"
  )

  lazy val loggingDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  )

  lazy val sparkVersion = "3.1.1"
  lazy val sparkDeps = Seq(
    "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly (),
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
  )

  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.3"

  lazy val testVersion = "3.2.0"
  lazy val testingDeps = Seq(
    "org.scalactic" %% "scalactic" % testVersion,
    "org.scalatest" %% "scalatest" % testVersion % "test"
  )

  lazy val typeSafeConfig = "com.typesafe" % "config" % "1.4.1"

}
