package io.opentargets.etl.literature

import com.typesafe.config.ConfigFactory
import pureconfig.ConfigReader.Result
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.spark.Helpers.IOResourceConfig
import pureconfig._
import pureconfig.generic.auto._

object Configuration extends LazyLogging {
  lazy val config: Result[OTConfig] = load

  case class Common(defaultSteps: Seq[String], output: String, outputFormat: String)

  case class ProcessingOutput(cooccurrences: IOResourceConfig, matches: IOResourceConfig)

  case class ProcessingSection(
      otLuts: IOResourceConfig,
      epmc: IOResourceConfig,
      outputs: ProcessingOutput
  )

  case class EmbeddingOutput(wordvec: IOResourceConfig,
                             wordvecsyn: IOResourceConfig,
                             literature: IOResourceConfig)

  case class EmbeddingSection(
      matches: IOResourceConfig,
      outputs: EmbeddingOutput
  )

  case class OTConfig(
      sparkUri: Option[String],
      common: Common,
      processing: ProcessingSection,
      embedding: EmbeddingSection
  )

  def load: ConfigReader.Result[OTConfig] = {
    logger.info("load configuration from file")
    val config = ConfigFactory.load()

    val obj = ConfigSource.fromConfig(config).load[OTConfig]
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }
}
