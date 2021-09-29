package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.linalg.Vectors._
import org.apache.spark.ml.linalg._
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
      StructField(name = "sharedPublicationCount", dataType = DoubleType, nullable = false),
      StructField(name = "meanTargetFreqPerPub", dataType = DoubleType, nullable = false),
      StructField(name = "meanDiseaseFreqPerPub", dataType = DoubleType, nullable = false)
    )
  )

  private def computeSimilarityScore(col1: Column, col2: Column): Column = {
    val cossim = udf((v1: Vector, v2: Vector) => {
      val n1 = norm(v1, 2D)
      val n2 = norm(v2, 2D)
      val denom = n1 * n2
      if (denom == 0.0) 0.0
      else (v1 dot v2) / denom
    })

    cossim(col1, col2)
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

    logger.info("Generate vector table from W2V model")
    val configuration = context.configuration.evidence

    val imap = Map(
      "matches" -> configuration.inputs.matches
    )
    val matches = Helpers.readFrom(imap).apply("matches").data
    val model = Word2VecModel.load(configuration.inputs.model)
    val eset = generateEvidence(model, matches, configuration.threshold)

    val dataframesToSave = Map(
      "evidence" -> IOResource(eset, configuration.output)
    )

    Helpers.writeTo(dataframesToSave)
  }
}
