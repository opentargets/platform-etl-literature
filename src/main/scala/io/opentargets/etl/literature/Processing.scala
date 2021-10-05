package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.storage.StorageLevel

object Processing extends Serializable with LazyLogging {
  private def maxHarmonicFn(s: Column): Column =
    aggregate(
      zip_with(sequence(lit(1), s), sequence(lit(1), s), (e1, e2) => e1 / pow(e2, 2D)),
      lit(0D),
      (c1, c2) => c1 + c2
    )

  private def harmonicFn(v: Column, s: Column): Column =
    aggregate(
      zip_with(v, sequence(lit(1), s), (e1, e2) => e1 / pow(e2, 2D)),
      lit(0D),
      (c1, c2) => c1 + c2
    )

  private def filterCooccurrences(df: DataFrame, isMapped: Boolean)(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val droppedCols = "co-occurrence" :: Nil

    df.selectExpr("*", "`co-occurrence`.*")
      .drop(droppedCols: _*)
      .filter($"isMapped" === isMapped)

  }

  private def filterMatches(df: DataFrame, isMapped: Boolean)(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val droppedCols = "match" :: Nil

    df.selectExpr("*", "match.*")
      .drop(droppedCols: _*)
      .filter($"isMapped" === isMapped)
  }

  private def filterMatchesForCH(df: DataFrame)(implicit context: ETLSessionContext): DataFrame = {
    import context.sparkSession.implicits._

    val sectionImportances = context.configuration.common.publicationSectionRanks
    val titleWeight = sectionImportances.withFilter(_.section == "title").map(_.weight).head

    val sectionRankTable =
      broadcast(
        sectionImportances
          .toDS()
          .orderBy($"rank".asc))

    val wBySectionKeyword = Window.partitionBy("pmid", "section", "keywordId")
    val wByKeyword = Window.partitionBy("pmid", "keywordId")

    val cols = List(
      "pmid",
      "pmcid",
      "date",
      "year",
      "month",
      "day",
      "keywordId",
      "relevance",
      "keywordType",
      "sentences"
    )

    val fdf = df
      .withColumn("pmid", $"pmid".cast(LongType))
      .withColumnRenamed("type", "keywordType")

    val sentencesDF = fdf
      .filter($"section".isInCollection(Seq("title", "abstract")))
      .groupBy($"pmid", $"section")
      .agg(
        struct(
          $"section",
          collect_list(
            struct($"label",
                   $"keywordType",
                   $"keywordId",
                   $"startInSentence",
                   $"endInSentence",
                   $"sectionStart",
                   $"sectionEnd")).as("matches")
        ).as("sentencesBySection")
      )
      .groupBy($"pmid")
      .agg(to_json(collect_list($"sentencesBySection")).as("sentences"))

    fdf
      .join(sectionRankTable, Seq("section"), "left_outer")
      .na
      .fill(100, "rank" :: Nil)
      .na
      .fill(0.01, "weight" :: Nil)
      .withColumn("keywordSectionV",
                  when($"section" =!= "title", collect_list($"weight").over(wBySectionKeyword))
                    .otherwise(array(lit(titleWeight))))
      .dropDuplicates("pmid", "section", "keywordId")
      .withColumn("relevanceV",
                  flatten(collect_list($"keywordSectionV").over(wByKeyword.orderBy($"rank".asc))))
      .withColumn("relevance", harmonicFn($"relevanceV", size($"relevanceV")))
      .dropDuplicates("pmid", "keywordId")
      .join(sentencesDF, Seq("pmid"), "left_outer")
      .selectExpr(cols: _*)
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
        first($"pmcid").as("pmcid"),
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

    logger.info("Processing raw evidences and persist matches and cooccurrences")

    ("matches" :: "cooccurrences" :: Nil) foreach { l =>
      grounding(l).persist(StorageLevel.DISK_ONLY)
    }

    val samples = grounding("samples")
    val failedMatches = grounding("matchesFailed")
    val failedCoocs = grounding("cooccurrencesFailed")

    logger.info("Processing matches calculate done")
    val matches = filterMatches(grounding("matches"), isMapped = true)
    // add disambiguate here perhaps?

    logger.info("Processing coOccurences calculate done")
    val coocs = filterCooccurrences(grounding("cooccurrences"), isMapped = true)

    val literatureIndexAlt = matches.transform(filterMatchesForCH)

    val outputs = empcConfiguration.outputs
    logger.info(s"write to ${context.configuration.common.output}/matches")
    val dataframesToSave = Map(
      "mappedLabels" -> IOResource(grounding("mappedLabels"), outputs.grounding),
      "samples" -> IOResource(samples, outputs.rawEvidence),
      "failedMatches" -> IOResource(
        failedMatches,
        outputs.matches.copy(path = context.configuration.common.output + "/failedMatches")),
      "failedCoocs" -> IOResource(
        failedCoocs,
        outputs.matches.copy(path = context.configuration.common.output + "/failedCooccurrences")),
      "cooccurrences" -> IOResource(coocs, outputs.cooccurrences),
      "matches" -> IOResource(matches, outputs.matches),
      "literatureIndex" -> IOResource(literatureIndexAlt, outputs.literatureIndex)
    )

    Helpers.writeTo(dataframesToSave)
    dataframesToSave
  }

}
