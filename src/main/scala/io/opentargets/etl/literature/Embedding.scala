package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.Configuration.ModelConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql.expressions.Window

object Embedding extends Serializable with LazyLogging {

  /*
    +------------------+---------+------------------------------+
    |section           |N        |count(struct(pmid, keywordId))|
    +------------------+---------+------------------------------+
    |abstract          |116503185|46271939                      |
    |results           |50031315 |10836737                      |
    |discuss           |43289023 |12340036                      |
    |other             |34434815 |9689403                       |
    |intro             |27842336 |11004578                      |
    |methods           |25213287 |11185594                      |
    |title             |16715850 |15051096                      |
    |table             |10836251 |5809557                       |
    |fig               |9647168  |2834907                       |
    |results,discuss   |5169808  |1338478                       |
    |concl             |3305883  |1660817                       |
    |case              |1643966  |1062965                       |
    |suppl             |1336509  |500587                        |
    |discuss,concl     |548469   |200646                        |
    |abbr              |444678   |338709                        |
    |appendix          |69560    |29606                         |
    |methods,results   |49600    |17177                         |
    |ack_fund          |45884    |34685                         |
    |methods,concl     |29243    |11603                         |
    |methods,discuss   |25208    |8736                          |
    |auth_cont         |24154    |18708                         |
    |comp_int          |15612    |10776                         |
   */
  val sectionImportanceTable: List[(String, Long)] =
    List(
      ("title", 1),
      ("concl", 2),
      ("abstract", 3),
      ("discuss,concl", 3),
      ("discuss", 4),
      ("results", 5),
      ("results,discuss", 5),
      ("methods,results", 7),
      ("methods,concl", 7),
      ("methods,discuss", 7)
    )

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

  private def transformMatches(selectCols: Seq[String], sectionRankingTable: DataFrame)(
      df: DataFrame): DataFrame = {

    val wByFreq = Window.partitionBy("pmid", "section", "keywordId")
    val w = Window.partitionBy("pmid").orderBy(col("importance").asc, col("f").desc)

    df.join(sectionRankingTable, Seq("section"), "left_outer")
      .na
      .fill(100, "importance" :: Nil)
      .withColumn("f", count(lit(1)).over(wByFreq))
      .dropDuplicates("pmid", "section", "keywordId")
      .withColumn("terms", collect_list(col("keywordId")).over(w))
      .selectExpr(selectCols: _*)
  }

  def compute(matches: DataFrame, configuration: Configuration.OTConfig)(
      implicit sparkSession: SparkSession): Map[String, IOResource] = {
    import sparkSession.implicits._

    val outputs = configuration.embedding.outputs
    val modelConf = configuration.embedding.modelConfiguration

    val sectionRankTable =
      broadcast(sectionImportanceTable.toDF("section", "importance").orderBy($"importance".asc))

    logger.info("CPUs available: " + Runtime.getRuntime().availableProcessors().toString())
    logger.info(s"Model configuration: ${modelConf.toString}")

    val groupedMatches = matches.transform(transformMatches("terms" :: Nil, sectionRankTable))

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
