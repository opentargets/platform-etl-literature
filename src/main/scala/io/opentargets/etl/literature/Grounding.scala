package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.Configuration.ProcessingSection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql._

object Grounding extends Serializable with LazyLogging {

  def resolveEntities(entities: DataFrame, luts: DataFrame)(
      implicit
      sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val mergedMatches = entities
      .withColumn("match", explode($"matches"))
      .drop("matches")
      .selectExpr("*", "match.*")
      .drop("match")
      .withColumn("labelN", Helpers.normalise($"label"))
      .join(luts, Seq("type", "labelN"), "left_outer")
      .withColumn("isMapped", $"keywordId".isNotNull)
      .groupBy($"pmid", $"text")
      .agg(
        first($"organisms").as("organisms"),
        first($"pubDate").as("pubDate"),
        first($"section").as("section"),
        collect_set(
          struct(
            $"endInSentence",
            $"label",
            $"sectionEnd",
            $"sectionStart",
            $"startInSentence",
            $"type",
            $"labelN",
            $"keywordId",
            $"isMapped"
          )
        ).as("matches")
      )

    val mergedCooc = entities
      .withColumn("cooc", explode($"co-occurrence"))
      .drop("co-occurrence")
      .selectExpr("*", "cooc.*")
      .drop("cooc")
      .withColumn("label1N", Helpers.normalise($"label1"))
      .withColumn("label2N", Helpers.normalise($"label2"))
      .withColumn("type1", substring_index($"type", "-", 1))
      .withColumn("type2", substring_index($"type", "-", -1))
      .drop("type")
      .join(luts, $"type1" === $"type" and $"label1N" === $"labelN", "left_outer")
      .withColumnRenamed("keywordId", "keywordId1")
      .drop("type", "labelN")
      .join(luts, $"type2" === $"type" and $"label2N" === $"labelN", "left_outer")
      .withColumnRenamed("keywordId", "keywordId2")
      .drop("type", "labelN")
      .withColumn("isMapped", $"keywordId1".isNotNull and $"keywordId2".isNotNull)
      .groupBy($"pmid", $"text")
      .agg(
        collect_set(
          struct(
            $"association",
            $"end1",
            $"end2",
            $"evidence_score",
            $"label1",
            $"keywordId1",
            $"label2",
            $"keywordId2",
            $"relation",
            $"start1",
            $"start2",
            concat_ws("-", $"type1", $"type2").as("type"),
            $"type1",
            $"type2",
            $"isMapped"
          )
        ).as("co-occurrence")
      )

    val merged =
      mergedMatches
        .join(mergedCooc, Seq("pmid", "text"), "left_outer")
        .groupBy($"pmid")
        .agg(
          first($"organisms").as("organisms"),
          first($"pubDate").as("pubDate"),
          collect_set(
            struct(
              $"co-occurrence",
              $"matches",
              $"section",
              $"text"
            )
          ).as("sentences")
        )

    merged
  }

  /* it generates a dataframe with a type (DS,GP,CD) in order to match EPMC info */
  def loadLUTs(df: DataFrame)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val selectedColumns = Seq(
      $"id".as("keywordId"),
      $"name",
      when($"entity" === "target", lit("GP"))
        .when($"entity" === "disease", lit("DS"))
        .when($"entity" === "drug", lit("CD"))
        .as("type"),
      $"keywords"
    )

    val data = df
      .select(selectedColumns: _*)
      .withColumn("keyword", explode($"keywords"))
      .withColumn("labelN", Helpers.normalise($"keyword"))
      .drop("keywords")
      .orderBy($"type", $"labelN")

    data
  }

  def loadEntities(df: DataFrame)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    df.withColumn("sentence", explode($"sentences"))
      .drop("sentences")
      .selectExpr("*", "sentence.*")
      .drop("sentence")

  }

  def compute(empcConfiguration: ProcessingSection)(
      implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession
    logger.info("Grounding step")

    val mappedInputs = Map(
      // search output of ETL. (disease,drug,target)
      "luts" -> empcConfiguration.otLuts,
      "epmc" -> empcConfiguration.epmc
    )

    val inputDataFrames = Helpers.readFrom(mappedInputs)
    val luts = broadcast(loadLUTs(inputDataFrames("luts").data))
    val entities = loadEntities(inputDataFrames("epmc").data)

    resolveEntities(entities, luts)
  }

}
