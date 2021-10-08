package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.{IOResource, makeWord2VecModel, writeTo}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.expressions.Window

object Embedding extends Serializable with LazyLogging {
  private def filterMatches(matches: DataFrame)(
      implicit etlSessionContext: ETLSessionContext): DataFrame = {
    import etlSessionContext.sparkSession.implicits._

    logger.info("prepare matches filtering by type of entities")

    val types = "DS" :: "GP" :: "CD" :: Nil
    matches
      .filter($"isMapped" === true and $"type".isInCollection(types))
  }

  private def regroupMatches(selectCols: Seq[String])(df: DataFrame)(
      implicit etlSessionContext: ETLSessionContext): DataFrame = {
    import etlSessionContext.sparkSession.implicits._

    logger.info("prepare matches regrouping the entities by ranked section")
    val sectionImportances =
      etlSessionContext.configuration.evidence.publicationSectionRanks
    val sectionRankTable =
      broadcast(
        sectionImportances
          .toDS()
          .orderBy($"rank".asc))

    val partitionPerSection = "pmid" :: "rank" :: Nil
    val wPerSection = Window.partitionBy(partitionPerSection.map(col): _*)

    val trDS = df
      .join(sectionRankTable, Seq("section"))
      .withColumn("keys", collect_set($"keywordId").over(wPerSection))
      .dropDuplicates(partitionPerSection.head, partitionPerSection.tail: _*)
      .groupBy($"pmid")
      .agg(collect_list($"keys").as("keys"))
      .withColumn("overall", flatten($"keys"))
      .withColumn("all", concat($"keys", array($"overall")))
      .withColumn("terms", explode($"all"))
      .selectExpr(selectCols: _*)
      .persist()

    logger.info("saving training dataset")
    writeTo(
      Map(
        "trainingSet" -> IOResource(
          trDS,
          etlSessionContext.configuration.embedding.output.trainingSet
        )
      )
    )(etlSessionContext.sparkSession)

    trDS

  }

  def generateModel(matches: DataFrame)(
      implicit etlSessionContext: ETLSessionContext): Word2VecModel = {
    val modelConfiguration = etlSessionContext.configuration.evidence.modelConfiguration
    val df = matches
      .transform(filterMatches)
      .transform(regroupMatches("pmid" :: "terms" :: Nil))

    logger.info(s"training W2V model with configuration ${modelConfiguration.toString}")
    makeWord2VecModel(df, modelConfiguration, inputColName = "terms", outputColName = "synonyms")
  }

  def compute(matches: DataFrame, configuration: Configuration.OTConfig)(
      implicit etlSessionContext: ETLSessionContext): Map[String, IOResource] = {

    val output = configuration.embedding.output
    val modelConf = configuration.embedding.modelConfiguration

    logger.info("CPUs available: " + Runtime.getRuntime().availableProcessors().toString())
    logger.info(s"Model configuration: ${modelConf.toString}")

    val matchesModels = generateModel(matches)

    // The matchesModel is a W2VModel and the output is parquet.
    matchesModels.save(output.path)

    val dataframesToSave = Map.empty[String, IOResource]
    dataframesToSave
  }

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Embedding step reading the files matches")
    val configuration = context.configuration

    val mappedInputs = Map(
      "matches" -> configuration.embedding.input
    )
    val inputDataFrames = Helpers.readFrom(mappedInputs)
    compute(inputDataFrames("matches").data, configuration)
  }
}
