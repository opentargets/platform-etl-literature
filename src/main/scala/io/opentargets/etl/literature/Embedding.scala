package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.Configuration.{ModelConfiguration, PublicationSectionRank}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.{IOResource, makeWord2VecModel}
import org.apache.spark.sql.expressions.Window

object Embedding extends Serializable with LazyLogging {
  private def generateSynonyms(matchesModel: Word2VecModel, numSynonyms: Int)(
      implicit
      sparkSession: SparkSession) = {
    import sparkSession.implicits._

    logger.info("produce the list of unique terms (GP, DS, CD)")
    val keywords = matchesModel.getVectors.selectExpr("word as keywordId")
    val bcModel = sparkSession.sparkContext.broadcast(matchesModel)

    logger.info(
      "compute the predictions to the associations DF with the precomputed model FPGrowth"
    )

    val matchesWithSynonymsFn = udf((word: String) => {
      try {
        bcModel.value.findSynonymsArray(word, numSynonyms)
      } catch {
        case _ => Array.empty[(String, Double)]
      }
    })

    val matchesWithSynonyms = keywords
      .withColumn("synonym", explode(matchesWithSynonymsFn($"keywordId")))
      .withColumn("synonymId", $"synonym".getField("_1"))
      .withColumn(
        "synonymType",
        when($"synonymId" rlike "^ENSG.*", "GP")
          .when($"synonymId" rlike "^CHEMBL.*", "CD")
          .otherwise("DS")
      )
      .withColumn(
        "keywordType",
        when($"keywordId" rlike "^ENSG.*", "GP")
          .when($"keywordId" rlike "^CHEMBL.*", "CD")
          .otherwise("DS")
      )
      .withColumn("synonymScore", $"synonym".getField("_2"))
      .drop("synonym")

    matchesWithSynonyms
  }

  private def generateWord2VecModel(df: DataFrame, modelConfiguration: ModelConfiguration)(
      implicit sparkSession: SparkSession) = {
    val matchesModel =
      makeWord2VecModel(df, modelConfiguration, inputColName = "terms", outputColName = "synonyms")

    matchesModel
  }

  def transformMatches(
      selectCols: Seq[String],
      sectionRankingTable: Dataset[PublicationSectionRank])(df: DataFrame): DataFrame = {

    val wByFreq = Window.partitionBy("pmid", "section", "keywordId")
    val w = Window.partitionBy("pmid").orderBy(col("rank").asc, col("f").desc)

    df.join(sectionRankingTable, Seq("section"), "left_outer")
      .na
      .fill(100, "rank" :: Nil)
      .withColumn("f", count(lit(1)).over(wByFreq))
      .dropDuplicates("pmid", "section", "keywordId")
      .withColumn("terms", collect_list(col("keywordId")).over(w))
      .selectExpr(selectCols: _*)
  }

  def compute(matches: DataFrame, configuration: Configuration.OTConfig)(
      implicit sparkSession: SparkSession): Map[String, IOResource] = {
    import sparkSession.implicits._

    val output = configuration.embedding.output
    val modelConf = configuration.embedding.modelConfiguration
    val sectionImportances =
      configuration.common.publicationSectionRanks

    val sectionRankTable =
      broadcast(
        sectionImportances
          .toDS()
          .orderBy($"rank".asc))

    logger.info("CPUs available: " + Runtime.getRuntime().availableProcessors().toString())
    logger.info(s"Model configuration: ${modelConf.toString}")

    val groupedMatches = matches.transform(transformMatches("terms" :: Nil, sectionRankTable))

    val matchesModels =
      generateWord2VecModel(groupedMatches, modelConf)

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
