import sbt._

object Dependencies {

  lazy val configDeps = Seq(
    "com.github.pureconfig" %% "pureconfig" % "0.14.1"
  )

  lazy val loggingDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3"
  )

  lazy val sparkVersion = "3.1.2"
  lazy val sparkDeps = Seq(
    "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly (),
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
  )

  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.3" % "test"

  lazy val testVersion = "3.2.0"
  lazy val testingDeps = Seq(
    "org.scalactic" %% "scalactic" % testVersion % "test",
    "org.scalatest" %% "scalatest" % testVersion % "test"
  )

  lazy val typeSafeConfig = "com.typesafe" % "config" % "1.4.1"

  lazy val johnSVersion = "3.3.4"
  lazy val johnS = Seq(
    "com.johnsnowlabs.nlp" % "spark-nlp_2.12" % johnSVersion
  )
}
