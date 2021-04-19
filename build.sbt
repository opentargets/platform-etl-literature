import Dependencies._

val buildResolvers = Seq(
  "Typesafe Repo" at "https://repo.typesafe.com/typesafe/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Bintray Repo" at "https://dl.bintray.com/spark-packages/maven/"
)

lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "io.opentargets",
        scalaVersion := "2.12.12"
      )
    ),
    name := "io-opentargets-etl-literature",
    version := "1.6",
    resolvers ++= buildResolvers,
    libraryDependencies ++= loggingDeps,
    libraryDependencies ++= sparkDeps,
    libraryDependencies ++= configDeps,
    libraryDependencies += scalaCheck,
    libraryDependencies ++= testingDeps,
    libraryDependencies += typeSafeConfig,
    libraryDependencies ++= johnS,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
        MergeStrategy.filterDistinctLines
      case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
        MergeStrategy.concat
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case _                           => MergeStrategy.first
    }
  )
