package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.Grounding.foldCooccurrences
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql._

object Processing extends Serializable with LazyLogging {

  private def filterCooccurrences(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    df.selectExpr("*", "`co-occurrence`.*")
      .drop("co-occurrence")
      .filter($"isMapped" === true)

  }

  private def filterMatches(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    df.selectExpr("*", "match.*")
      .drop("match")
      .filter($"isMapped" === true)
  }

  def apply()(implicit context: ETLSessionContext): Map[String, IOResource] = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Processing step")

    val empcConfiguration = context.configuration.processing
    val grounding = Grounding.compute(empcConfiguration)

    val rawEvidences = foldCooccurrences(grounding("cooccurrences"))
    logger.info("Processing raw evidences")

    val coocs = filterCooccurrences(grounding("cooccurrences"))
    logger.info("Processing coOccurences calculate done")

    val matches = filterMatches(grounding("matches"))
    logger.info("Processing matches calculate done")

    val outputs = empcConfiguration.outputs
    logger.info(s"write to ${context.configuration.common.output}/matches")
    val dataframesToSave = Map(
      "rawEvidences" -> IOResource(rawEvidences, outputs.rawEvidence),
      "cooccurrences" -> IOResource(coocs, outputs.cooccurrences),
      "matches" -> IOResource(matches, outputs.matches)
    )

    Helpers.writeTo(dataframesToSave)
    dataframesToSave
  }

}
