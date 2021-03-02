package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql._

object Processing extends Serializable with LazyLogging {

  private def coOccurrences(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    df.withColumn("sentence", explode($"sentences"))
      .selectExpr("*", "sentence.*")
      .drop("sentence", "sentences", "matches")
      .filter($"co-occurrence".isNotNull)
      .withColumn("cooc", explode($"co-occurrence"))
      .selectExpr("*", "cooc.*")
      .drop("cooc", "co-occurrence")

  }

  private def createIndexForETL(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    df.filter($"isMapped" === true)
      .groupBy($"pmid", $"type")
      .agg(
        first($"organisms").as("organisms"),
        first($"pubDate").as("pubDate"),
        first($"section").as("section"),
        first($"text").as("text"),
        collect_list(
          struct(
            $"endInSentence",
            $"label",
            $"sectionEnd",
            $"sectionStart",
            $"startInSentence",
            $"labelN",
            $"keywordId",
            $"isMapped"
          )
        ).as("matches")
      )

  }
  private def matches(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    df.withColumn("sentence", explode($"sentences"))
      .selectExpr("*", "sentence.*")
      .drop("sentence", "sentences", "co-occurrence")
      .filter($"matches".isNotNull)
      .withColumn("match", explode($"matches"))
      .selectExpr("*", "match.*")
      .drop("match", "matches")

  }

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Processing step")

    val empcConfiguration = context.configuration.processing

    val grounding = Grounding.compute(empcConfiguration)

    val epmcCoOccurrencesDf = coOccurrences(grounding)
    val matchesDf = matches(grounding)
    val literatureETL = createIndexForETL(matchesDf)

    val outputs = empcConfiguration.outputs
    logger.info(s"write to ${context.configuration.common.output}/matches")
    val dataframesToSave = Map(
      "cooccurrences" -> IOResource(epmcCoOccurrencesDf, outputs.cooccurrences),
      "matches" -> IOResource(matchesDf, outputs.matches)
    )

    literatureETL.write.json("literature")
    Helpers.writeTo(dataframesToSave)
  }

}
