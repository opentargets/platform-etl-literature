package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.{
  IOResource,
  IOResourceConfig,
  computeSimilarityScore,
  makeWord2VecModel,
  writeTo
}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object Evidence extends Serializable with LazyLogging {
  val schema: StructType = StructType(
    Array(
      StructField(name = "datasourceId", dataType = StringType, nullable = false),
      StructField(name = "datatypeId", dataType = StringType, nullable = false),
      StructField(name = "targetFromSourceId", dataType = StringType, nullable = false),
      StructField(name = "diseaseFromSourceMappedId", dataType = StringType, nullable = false),
      StructField(name = "resourceScore", dataType = DoubleType, nullable = false),
      StructField(name = "sharedPublicationCount", dataType = IntegerType, nullable = false),
      StructField(name = "meanTargetFreqPerPub", dataType = DoubleType, nullable = false),
      StructField(name = "meanDiseaseFreqPerPub", dataType = DoubleType, nullable = false)
    )
  )

  private def filterMatches(matches: DataFrame)(
      implicit etlSessionContext: ETLSessionContext): DataFrame = {
    import etlSessionContext.sparkSession.implicits._

    val types = "DS" :: "GP" :: "CD" :: Nil
    matches
      .filter($"isMapped" === true and $"type".isInCollection(types))
  }

  private def regroupMatches(selectCols: Seq[String])(df: DataFrame)(
      implicit etlSessionContext: ETLSessionContext): DataFrame = {
    import etlSessionContext.sparkSession.implicits._

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
      .withColumn("terms", collect_set($"keywordId").over(wPerSection))
      .dropDuplicates(partitionPerSection.head, partitionPerSection.tail: _*)
      .groupBy($"pmid")
      .agg(collect_list($"rank").as("ranks"))
      .withColumn("overall", flatten($"ranks"))
      .withColumn("all", concat($"ranks", array($"overall")))
      .withColumn("terms", explode($"all"))
      .selectExpr(selectCols: _*)
      .persist()

    logger.info("saving training dataset")
    writeTo(
      Map(
        "trainingSet" -> IOResource(
          trDS,
          etlSessionContext.configuration.evidence.outputs.evidenceTrainingSet
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

    makeWord2VecModel(df, modelConfiguration, inputColName = "terms", outputColName = "synonyms")
  }

  def generateEvidence(model: Word2VecModel, matches: DataFrame, threshold: Option[Double])(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val gcols = List("pmid", "type", "keywordId")
    logger.info("filter diseases from the matches")
    val mWithV = matches
      .filter($"isMapped" === true)
      .groupBy(gcols.map(col): _*)
      .agg(count($"pmid").as("f"))
      .join(model.getVectors, $"word" === $"keywordId")
      .drop("word")

    val matchesDS = mWithV
      .filter($"type" === "DS")
      .drop("type")
      .withColumnRenamed("keywordId", "diseaseFromSourceMappedId")
      .withColumnRenamed("f", "diseaseF")
      .withColumnRenamed("vector", "diseaseV")
      .withColumnRenamed("vector", "diseaseV")
      .withColumnRenamed("pmid", "diseaseP")

    val matchesGP = mWithV
      .filter($"type" === "GP")
      .drop("type")
      .withColumnRenamed("keywordId", "targetFromSourceId")
      .withColumnRenamed("f", "targetF")
      .withColumnRenamed("vector", "targetV")
      .withColumnRenamed("pmid", "targetP")

    val ev = matchesDS
      .join(
        matchesGP,
        ($"targetP" === $"diseaseP") and ($"diseaseFromSourceMappedId" !== $"targetFromSourceId"),
        "inner")
      .groupBy($"targetFromSourceId", $"diseaseFromSourceMappedId")
      .agg(
        first($"targetV").as("targetV"),
        first($"diseaseV").as("diseaseV"),
        mean($"targetF").as("meanTargetFreqPerPub"),
        mean($"diseaseF").as("meanDiseaseFreqPerPub"),
        count($"targetP").as("sharedPublicationCount")
      )
      .withColumn("resourceScore", computeSimilarityScore($"targetV", $"diseaseV"))
      .filter($"resourceScore" > threshold.getOrElse(Double.MinPositiveValue))
      .withColumn("datasourceId", lit("ew2v"))
      .withColumn("datatypeId", lit("literature"))
      .select(schema.fieldNames.map(col): _*)

    ev
  }

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    val configuration = context.configuration.evidence

    val imap = Map(
      "matches" -> configuration.input
    )

    val matches = Helpers.readFrom(imap).apply("matches").data

    val w2vModel = configuration.skipModel.getOrElse(false) match {
      case true =>
        logger.info(s"Load w2v model from path ${configuration.outputs.model.path}")
        Word2VecModel.load(configuration.outputs.model.path)
      case false =>
        logger.info(s"Generate w2v model and save to path ${configuration.outputs.model.path}")
        val m = generateModel(matches)
        m.save(configuration.outputs.model.path)
        m
    }

    logger.info("Generate evidence set from w2v model")
    val eset = generateEvidence(w2vModel, matches, configuration.threshold)
    val dataframesToSave = Map(
      "evidence" -> IOResource(eset, configuration.outputs.evidence)
    )

    Helpers.writeTo(dataframesToSave)
  }
}
