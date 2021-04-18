package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.Embedding.logger
import io.opentargets.etl.literature.Grounding.foldCooccurrences
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object Processing extends Serializable with LazyLogging {

  private def filterCooccurrences(df: DataFrame, isMapped: Boolean)(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val droppedCols = "co-occurrence" +: (if (isMapped)
                                            df.columns.filter(_.startsWith("trace_")).toList
                                          else List.empty)

    df.selectExpr("*", "`co-occurrence`.*")
      .drop(droppedCols: _*)
      .filter($"isMapped" === isMapped)

  }

  private def filterMatches(df: DataFrame, isMapped: Boolean)(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val droppedCols = "match" +: (if (isMapped)
                                    df.columns.filter(_.startsWith("trace_")).toList
                                  else List.empty)

    df.selectExpr("*", "match.*")
      .drop(droppedCols: _*)
      .filter($"isMapped" === isMapped)
  }

  private def aggregateMatches(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val countsPerKey = df
      .filter($"section".isNotNull and $"isMapped" === true)
      .withColumn("pubDate", $"date")
      .select($"pmid", $"pmcid", $"keywordId", $"pubDate", $"organisms")
      .groupBy($"pmid", $"keywordId")
      .agg(
        first($"pmcid").as("pmcid"),
        first($"pubDate").as("pubDate"),
        first($"organisms").as("organisms"),
        count($"keywordId").as("countsPerKey")
      )
      .groupBy($"pmid")
      .agg(
        first($"pubDate").as("pubDate"),
        first($"organisms").as("organisms"),
        collect_set(struct($"keywordId", $"countsPerKey")).as("countsPerTerm"),
        collect_set($"keywordId").as("terms")
      )

    logger.info(s"create literature-etl index for ETL")
    val aggregated = df
      .filter(
        $"section".isNotNull and
          $"isMapped" === true and
          $"section".isInCollection(Seq("title", "abstract")))
      .withColumn("match",
                  struct($"endInSentence",
                         $"label",
                         $"sectionEnd",
                         $"sectionStart",
                         $"startInSentence",
                         $"type",
                         $"keywordId",
                         $"isMapped"))
      .groupBy($"pmid", $"section")
      .agg(
        array_distinct(collect_list($"match")).as("matches")
      )
      .groupBy($"pmid")
      .agg(
        collect_list(struct($"section", $"matches")).as("sentences")
      )

    countsPerKey.join(aggregated, Seq("pmid"), "left_outer")
  }

  def apply()(implicit context: ETLSessionContext): Map[String, IOResource] = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Processing step")

    val empcConfiguration = context.configuration.processing
    val grounding = Grounding.compute(empcConfiguration)

//    val rawEvidences = foldCooccurrences(grounding("cooccurrences"))
    logger.info("Processing raw evidences")

    val failedMatches = filterMatches(grounding("matches"), isMapped = false)
    val failedCoocs = filterCooccurrences(grounding("cooccurrences"), isMapped = false)

    logger.info("Processing matches calculate done")
    val matches = filterMatches(grounding("matches"), isMapped = true)

    logger.info("Processing coOccurences calculate done")
    val coocs = filterCooccurrences(grounding("cooccurrences"), isMapped = true)

    val literatureIndex = matches.transform(aggregateMatches)

    val outputs = empcConfiguration.outputs
    logger.info(s"write to ${context.configuration.common.output}/matches")
    val dataframesToSave = Map(
      // "rawEvidences" -> IOResource(rawEvidences, outputs.rawEvidence),
      "failedMatches" -> IOResource(
        failedMatches,
        outputs.matches.copy(path = context.configuration.common.output + "/failedMatches")),
      "failedCoocs" -> IOResource(
        failedCoocs,
        outputs.matches.copy(path = context.configuration.common.output + "/failedCooccurrences")),
      "cooccurrences" -> IOResource(coocs, outputs.cooccurrences),
      "matches" -> IOResource(matches, outputs.matches),
      "literatureIndex" -> IOResource(literatureIndex, outputs.literatureIndex)
    )

    Helpers.writeTo(dataframesToSave)
    dataframesToSave
  }

}
