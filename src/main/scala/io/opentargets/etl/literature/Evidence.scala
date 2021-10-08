package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.{IOResource, computeSimilarityScore, harmonicFn}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Evidence extends Serializable with LazyLogging {
  val schema: StructType = StructType(
    Array(
      StructField(name = "datasourceId", dataType = StringType, nullable = false),
      StructField(name = "datatypeId", dataType = StringType, nullable = false),
      StructField(name = "targetFromSourceId", dataType = StringType, nullable = false),
      StructField(name = "diseaseFromSourceMappedId", dataType = StringType, nullable = false),
      StructField(name = "resourceScore", dataType = DoubleType, nullable = false),
      StructField(name = "rawResourceScore", dataType = DoubleType, nullable = false),
      StructField(name = "sharedPublicationCount", dataType = IntegerType, nullable = false),
      StructField(name = "meanTargetFreqPerPub", dataType = DoubleType, nullable = false),
      StructField(name = "meanDiseaseFreqPerPub", dataType = DoubleType, nullable = false)
    )
  )

  def generateEvidence(
      model: Word2VecModel,
      matches: DataFrame,
      coocs: DataFrame,
      threshold: Option[Double])(implicit etlSessionContext: ETLSessionContext): DataFrame = {
    import etlSessionContext.sparkSession.implicits._

    val sectionImportances =
      etlSessionContext.configuration.common.publicationSectionRanks
    val sectionRankTable =
      broadcast(
        sectionImportances
          .toDS()
          .orderBy($"rank".asc))

    val gcols = List("pmid", "type", "keywordId")
    logger.info("filter diseases from the matches")
    val mWithV = matches
      .filter($"isMapped" === true)
      .join(sectionRankTable, Seq("section"))
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
      .withColumn("rawResourceScore", computeSimilarityScore($"targetV", $"diseaseV"))
      .filter($"rawResourceScore" > threshold.getOrElse(Double.MinPositiveValue))
      .withColumn("sharedPublicationCount", $"sharedPublicationCount".cast(IntegerType))
      .withColumn("resourceScore",
                  harmonicFn(array_repeat($"rawResourceScore", $"sharedPublicationCount")))
      .withColumn("datasourceId", lit("ew2v"))
      .withColumn("datatypeId", lit("literature"))
      .select(schema.fieldNames.map(col): _*)

    ev
  }

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    val configuration = context.configuration.evidence

    val imap = Map(
      "matches" -> configuration.inputs.matches,
      "coocs" -> configuration.inputs.cooccurrences
    )

    val matches = Helpers.readFrom(imap).apply("matches").data
    val coocs = Helpers.readFrom(imap).apply("coocs").data

    logger.info(s"Load w2v model from path ${configuration.inputs.model.path}")
    val m = Word2VecModel.load(configuration.inputs.model.path)

    logger.info("Generate evidence set from w2v model")
    val eset = generateEvidence(m, matches, coocs, configuration.threshold)
    val dataframesToSave = Map(
      "evidence" -> IOResource(eset, configuration.output)
    )

    Helpers.writeTo(dataframesToSave)
  }
}
