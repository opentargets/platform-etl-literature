import sbt._

object Dependencies {

  lazy val configDeps = Seq(
    "org.yaml" % "snakeyaml" % "1.21",
    "com.github.pureconfig" %% "pureconfig" % "0.12.3"
  )

  lazy val loggingDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  )

  lazy val sparkVersion = "3.0.1"
  lazy val sparkDeps = Seq(
    "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly (),
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-graphx" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion
  )

  lazy val typeSafeConfig = "com.typesafe" % "config" % "1.4.0"
}
