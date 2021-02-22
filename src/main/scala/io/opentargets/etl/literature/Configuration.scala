package io.opentargets.etl.literature

import com.typesafe.config.ConfigFactory
import pureconfig.ConfigReader.Result
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.spark.Helpers.IOResourceConfig
import pureconfig._
import pureconfig.generic.auto._

object Configuration extends LazyLogging {
  lazy val config: Result[OTConfig] = load

  case class Common(defaultSteps: Seq[String],output: String, outputFormat: String)

  case class GroundingOutput( grounding: IOResourceConfig )

  case class GroundingSection(
                         otLuts: IOResourceConfig,
                         epmc: IOResourceConfig,
                         outputs: GroundingOutput
                        )

  case class AnalysisOutput( cooccurrences: IOResourceConfig, matches: IOResourceConfig )

  case class AnalysisSection(
                               grounding : IOResourceConfig,
                               outputs: AnalysisOutput
                             )

  case class EmbeddingOutput( wordvec: IOResourceConfig, wordvecsyn: IOResourceConfig )

  case class EmbeddingSection(
                              matches : IOResourceConfig,
                              outputs: EmbeddingOutput
                            )

  case class OTConfig(
                       sparkUri: Option[String],
                       common: Common,
                       grounding: GroundingSection,
                       analysis: AnalysisSection,
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
