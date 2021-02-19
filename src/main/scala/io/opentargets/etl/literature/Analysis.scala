package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql._

object Analysis extends Serializable with LazyLogging {

  private def coOccurrences(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    df.withColumn("sentence", explode($"sentences"))
      .selectExpr("*", "sentence.*").drop("sentence", "sentences", "matches")
      .filter($"co-occurrence".isNotNull)
      .withColumn("cooc", explode($"co-occurrence"))
      .selectExpr("*", "cooc.*").drop("cooc", "co-occurrence")

  }

  private def matches(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    df.withColumn("sentence", explode($"sentences"))
      .selectExpr("*", "sentence.*").drop("sentence", "sentences", "co-occurrence")
      .filter($"matches".isNotNull).withColumn("match", explode($"matches"))
      .selectExpr("*", "match.*").drop("match", "matches")

  }
  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Analysis step")

    val empcConfiguration = context.configuration.analysis

    val mappedInputs = Map(
      // search output of ETL. (disease,drug,target)
      "grounding" -> empcConfiguration.grounding
    )

    val inputDataFrames = Helpers.readFrom(mappedInputs)
    val epmcCoOccurrencesDf = coOccurrences(inputDataFrames("grounding").data)
    val matchesDf = matches(inputDataFrames("grounding").data)

    val outputs = context.configuration.analysis.outputs
    logger.info(s"write to ${context.configuration.common.output}/analysis")
    val dataframesToSave = Map(
      "cooccurrences" -> IOResource(epmcCoOccurrencesDf, outputs.cooccurrences),
      "matches" -> IOResource(matchesDf, outputs.matches)
    )

    Helpers.writeTo(dataframesToSave)
  }

}
