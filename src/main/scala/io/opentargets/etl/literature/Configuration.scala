package io.opentargets.etl.literature

import com.typesafe.config.ConfigFactory
import pureconfig.ConfigReader.Result
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.spark.Helpers.IOResourceConfig
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.auto._

object Configuration extends LazyLogging {
  lazy val config: Result[OTConfig] = load

  case class PublicationSectionRank(section: String, rank: Long, weight: Double)
  case class Common(defaultSteps: Seq[String],
                    output: String,
                    outputFormat: String,
                    publicationSectionRanks: Seq[PublicationSectionRank])

  case class ProcessingOutput(rawEvidence: IOResourceConfig,
                              cooccurrences: IOResourceConfig,
                              matches: IOResourceConfig,
                              literatureIndex: IOResourceConfig)

  case class ProcessingSection(
      epmcids: IOResourceConfig,
      diseases: IOResourceConfig,
      targets: IOResourceConfig,
      drugs: IOResourceConfig,
      epmc: IOResourceConfig,
      outputs: ProcessingOutput
  )

  case class EmbeddingOutput(wordvec: IOResourceConfig, wordvecsyn: IOResourceConfig)

  case class ModelConfiguration(windowSize: Int,
                                numPartitions: Int,
                                maxIter: Int,
                                minCount: Int,
                                stepSize: Double)

  case class EmbeddingSection(
      modelConfiguration: ModelConfiguration,
      numSynonyms: Int,
      input: IOResourceConfig,
      outputs: EmbeddingOutput
  )

  case class VectorsSection(input: String, output: IOResourceConfig)

  case class OTConfig(
      sparkUri: Option[String],
      common: Common,
      processing: ProcessingSection,
      embedding: EmbeddingSection,
      vectors: VectorsSection
  )

  def load: ConfigReader.Result[OTConfig] = {
    logger.info("load configuration from file")
    val config = ConfigFactory.load()

    val obj = ConfigSource.fromConfig(config).load[OTConfig]
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }
}
