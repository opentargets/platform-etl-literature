package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.Configuration.ModelConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource

object Embedding extends Serializable with LazyLogging {

  private def makeWord2VecModel(
      df: DataFrame,
      modelConfiguration: ModelConfiguration,
      inputColName: String,
      outputColName: String = "prediction"
  ): Word2VecModel = {
    logger.info(s"compute Word2Vec model for input col ${inputColName} into ${outputColName}")

    val w2vModel = new Word2Vec()
      .setWindowSize(modelConfiguration.windowSize)
      .setNumPartitions(modelConfiguration.numPartitions)
      .setMaxIter(modelConfiguration.maxIter)
      .setMinCount(modelConfiguration.minCount)
      .setStepSize(modelConfiguration.stepSize)
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    val model = w2vModel.fit(df)

    // Display frequent itemsets.
    //model.getVectors.show(25, false)

    model
  }

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
        case _: Throwable => Array.empty[(String, Double)]
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

  private def transformMatches(selectCols: String*)(df: DataFrame): DataFrame =
    df.groupBy(col("pmid"), col("section"))
      .agg(collect_set(col("keywordId")).as("ids"))
      .groupBy(col("pmid"))
      .agg(collect_list(col("ids")).as("ids"))
      .withColumn("all_ids", flatten(col("ids")))
      .withColumn("ids_v", concat(array(col("all_ids")), col("ids")))
      .withColumn("terms", explode(col("ids_v")))
      .selectExpr(selectCols: _*)

  def compute(matches: DataFrame, configuration: Configuration.OTConfig)(
      implicit sparkSession: SparkSession): Map[String, IOResource] = {
    val outputs = configuration.embedding.outputs
    val modelConf = configuration.embedding.modelConfiguration

    logger.info("CPUs available: " + Runtime.getRuntime().availableProcessors().toString())
    logger.info(s"Model configuration: ${modelConf.toString}")

    val groupedMatches = matches.transform(transformMatches("terms"))

    val matchesModels =
      generateWord2VecModel(groupedMatches, modelConf)
    val matchesSynonyms =
      generateSynonyms(matchesModels, configuration.embedding.numSynonyms)

    // The matchesModel is a W2VModel and the output is parquet.
    matchesModels.save(outputs.wordvec.path)

    logger.info(s"write synonyms computation")
    val dataframesToSave = Map(
      "word2vecSynonym" -> IOResource(matchesSynonyms, outputs.wordvecsyn)
    )
    Helpers.writeTo(dataframesToSave)

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
