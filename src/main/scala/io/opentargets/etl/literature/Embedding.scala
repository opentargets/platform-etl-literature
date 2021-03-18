package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object Embedding extends Serializable with LazyLogging {



  private def makeWord2VecModel(
      df: DataFrame,
      numPartitions: Int,
      inputColName: String,
      outputColName: String = "prediction"
  ): Word2VecModel = {
    logger.info(s"compute Word2Vec model for input col ${inputColName} into ${outputColName}")

    val w2vModel = new Word2Vec()
      .setWindowSize(5)
      .setNumPartitions(numPartitions)
      .setMaxIter(1)
      .setMinCount(3)
      .setStepSize(0.025)
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

  private def generateWord2VecModel(df: DataFrame, numPartitions: Int)(
      implicit sparkSession: SparkSession) = {
    val matchesModel =
      makeWord2VecModel(df, numPartitions, inputColName = "terms", outputColName = "synonyms")

    matchesModel
  }

  def compute(literatureIndex: DataFrame, configuration: Configuration.OTConfig)(
      implicit sparkSession: SparkSession): Map[String, IOResource] = {
    val outputs = configuration.embedding.outputs

    logger.info("CPUs available: " + Runtime.getRuntime().availableProcessors().toString())
    logger.info("Number of partitions: " + configuration.common.partitions.toString())

    val matchesModels =
      generateWord2VecModel(literatureIndex.select("terms"), configuration.common.partitions)
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
    import ss.implicits._

    logger.info("Embedding step reading the files matches")
    val configuration = context.configuration

    val mappedInputs = Map(
      "matches" -> configuration.embedding.input
    )
    val inputDataFrames = Helpers.readFrom(mappedInputs)
    compute(inputDataFrames("matches").data, configuration)
  }
}
