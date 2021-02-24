package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.storage.StorageLevel
import io.opentargets.etl.literature.spark.Helpers

object Embedding extends Serializable with LazyLogging {

  private def makeWord2VecModel(
      df: DataFrame,
      inputColName: String,
      outputColName: String = "prediction"
  ): Word2VecModel = {
    logger.info(s"compute Word2Vec model for input col ${inputColName} into ${outputColName}")

    val w2vModel = new Word2Vec()
      .setNumPartitions(32)
      .setMaxIter(10)
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    val model = w2vModel.fit(df)

    // Display frequent itemsets.
    //model.getVectors.show(25, false)

    model
  }

  private def generateSynonyms(df: DataFrame, matchesModel: Word2VecModel)(
      implicit
      sparkSession: SparkSession) = {
    import sparkSession.implicits._

    logger.info("produce the list of unique terms (GP, DS, CD)")
    val keywords = df
      .select($"keywordId")
      .distinct()

    val bcModel = sparkSession.sparkContext.broadcast(matchesModel)

    logger.info(
      "compute the predictions to the associations DF with the precomputed model FPGrowth"
    )
    val matchesWithSynonymsFn = udf((word: String) => {
      try {
        bcModel.value.findSynonymsArray(word, 50)
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

  private def generateWord2VecModel(df: DataFrame)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val mDF = df.filter($"isMapped" === true)

    val matchesPerPMID = mDF
      .groupBy($"pmid")
      .agg(collect_list($"keywordId").as("terms"))

    val matchesModel =
      makeWord2VecModel(matchesPerPMID, inputColName = "terms", outputColName = "synonyms")

    matchesModel

  }

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession
    import ss.implicits._

    logger.info("Embedding step")

    val empcConfiguration = context.configuration.embedding

    val mappedInputs = Map(
      // output of Analysis step. Matches data
      "matches" -> empcConfiguration.matches
    )

    val inputDataFrames = Helpers.readFrom(mappedInputs)
    val matchesFiltered = inputDataFrames("matches").data.filter($"isMapped" === true)
    val matchesModels = generateWord2VecModel(matchesFiltered)
    val matchesSynonyms = generateSynonyms(matchesFiltered, matchesModels)

    val outputs = context.configuration.embedding.outputs
    // To do: change output approach
    matchesModels.save(outputs.wordvec.path)
    matchesSynonyms.write.format(outputs.wordvecsyn.format).save(outputs.wordvecsyn.path)
  }

}
