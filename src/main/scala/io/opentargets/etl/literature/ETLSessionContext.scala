package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.spark.Helpers.getOrCreateSparkSession
import org.apache.spark.sql.SparkSession
import io.opentargets.etl.literature.Configuration.OTConfig
import pureconfig.error.ConfigReaderFailures


case class ETLSessionContext(configuration: OTConfig, sparkSession: SparkSession)

object ETLSessionContext extends LazyLogging {
  val progName: String = "ot-platform-etl-literature"

  def apply(): Either[ConfigReaderFailures , ETLSessionContext] = {
    for {
      config <- Configuration.config
    } yield ETLSessionContext(config, getOrCreateSparkSession(progName, config.sparkUri))
  }
}
