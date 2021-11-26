package io.opentargets.etl.literature.spark

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.Configuration.{ModelConfiguration, OTConfig}
import com.johnsnowlabs.nlp.annotator.{
  DocumentNormalizer,
  SentenceDetector,
  SentenceDetectorDLApproach
}
import com.johnsnowlabs.nlp.annotators.sentence_detector_dl.SentenceDetectorDLModel
import com.johnsnowlabs.nlp.base.{DocumentAssembler, Finisher}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors.norm
import org.apache.spark.sql._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.Random

object Helpers extends LazyLogging {
  type IOResourceConfigurations = Map[String, IOResourceConfig]
  type IOResources = Map[String, IOResource]

  case class IOResource(data: DataFrame, configuration: IOResourceConfig)
  case class IOResourceConfigOption(k: String, v: String)
  case class IOResourceConfig(
      format: String,
      path: String,
      options: Option[Seq[IOResourceConfigOption]] = None,
      partitionBy: Option[Seq[String]] = None
  )

  /** generate a spark session given the arguments if sparkUri is None then try to get from env
    * otherwise it will set the master explicitely
    * @param appName the app name
    * @param sparkUri uri for the spark env master if None then it will try to get from yarn
    * @return a sparksession object
    */
  def getOrCreateSparkSession(appName: String, sparkUri: Option[String]): SparkSession = {
    logger.info(s"create spark session with uri:'${sparkUri.toString}'")
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.driver.maxResultSize", "0")
      .set("spark.debug.maxToStringFields", "2000")
      .set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")

    // if some uri then setmaster must be set otherwise
    // it tries to get from env if any yarn running
    val conf = sparkUri match {
      case Some(uri) if uri.nonEmpty => sparkConf.setMaster(uri)
      case _                         => sparkConf
    }

    SparkSession.builder
      .config(conf)
      .getOrCreate
  }

  def makeWord2VecModel(
      df: DataFrame,
      modelConfiguration: ModelConfiguration,
      inputColName: String,
      outputColName: String = "prediction"
  ): Word2VecModel = {
    logger.info(s"compute Word2Vec model for input col $inputColName into $outputColName")

    val w2vModel = new Word2Vec()
      .setWindowSize(modelConfiguration.windowSize)
      .setNumPartitions(modelConfiguration.numPartitions)
      .setMaxIter(modelConfiguration.maxIter)
      .setMinCount(modelConfiguration.minCount)
      .setStepSize(modelConfiguration.stepSize)
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    val model = w2vModel.fit(df)

    model
  }

  def computeSimilarityScore(col1: Column, col2: Column): Column = {
    val cossim = udf((v1: Vector, v2: Vector) => {
      val n1 = norm(v1, 2D)
      val n2 = norm(v2, 2D)
      val denom = n1 * n2
      if (denom == 0.0) 0.0
      else (v1 dot v2) / denom
    })

    cossim(col1, col2)
  }

  def normalise(c: Column): Column = {
    // https://www.rapidtables.com/math/symbols/greek_alphabet.html
    translate(c, "αβγδεζηικλμνξπτυω", "abgdezhiklmnxptuo")
  }

  def harmonicFn(c: Column): Column =
    aggregate(
      zip_with(sort_array(c, asc = false), sequence(lit(1), size(c)), (e1, e2) => e1 / pow(e2, 2d)),
      lit(0d),
      (c1, c2) => c1 + c2
    )

  /** It creates an hashmap of dataframes.
    *   Es. inputsDataFrame {"disease", Dataframe} , {"target", Dataframe}
    *   Reading is the first step in the pipeline
    */
  def readFrom(
      inputFileConf: IOResourceConfigurations
  )(implicit session: SparkSession): IOResources = {
    logger.info("Load files into a Map of names and IOResource")
    for {
      (key, formatAndPath) <- inputFileConf
    } yield key -> IOResource(loadFileToDF(formatAndPath), formatAndPath)
  }

  def loadFileToDF(pathInfo: IOResourceConfig)(implicit session: SparkSession): DataFrame = {
    logger.info(s"load dataset ${pathInfo.path} with ${pathInfo.toString}")

    pathInfo.options
      .foldLeft(session.read.format(pathInfo.format)) {
        case ops =>
          val options = ops._2.map(c => c.k -> c.v).toMap
          ops._1.options(options)
      }
      .load(pathInfo.path)
  }

  def renameAllCols(schema: StructType, fn: String => String): StructType = {

    def renameDataType(dt: StructType): StructType =
      StructType(dt.fields.map {
        case StructField(name, dataType, nullable, metadata) =>
          val renamedDT = dataType match {
            case st: StructType => renameDataType(st)
            case ArrayType(elementType: StructType, containsNull) =>
              ArrayType(renameDataType(elementType), containsNull)
            case rest: DataType => rest
          }
          StructField(fn(name), renamedDT, nullable, metadata)
      })

    renameDataType(schema)
  }

  /** generate snake to camel for the Elasticsearch indices.
    * Replace all _ with Capiltal letter except the first letter. Eg. "abc_def_gh" => "abcDefGh"
    * @param df Dataframe
    * @return a DataFrame with the schema lowerCamel
    */
  def snakeToLowerCamelSchema(df: DataFrame)(implicit session: SparkSession): DataFrame = {

    //replace all _ with Capiltal letter except the first letter. Eg. "abc_def_gh" => "abcDefGh"
    val snakeToLowerCamelFnc = (s: String) => {
      val tokens = s.split("_")
      tokens.head + tokens.tail.map(_.capitalize).mkString
    }

    val newDF =
      session.createDataFrame(df.rdd, renameAllCols(df.schema, snakeToLowerCamelFnc))

    newDF
  }

  // Replace the spaces from the schema fields with _
  def replaceSpacesSchema(df: DataFrame)(implicit session: SparkSession): DataFrame = {

    //replace all spaces with _
    val renameFcn = (s: String) => s.replaceAll(" ", "_")

    val newDF =
      session.createDataFrame(df.rdd, renameAllCols(df.schema, renameFcn))

    newDF
  }

  /** Helper function to prepare multiple files of the same category to be read by `readFrom`
    * @param resourceConfigs collection of IOResourceConfig of unknown composition
    * @return Map with random keys to input resource.
    */
  def seqToIOResourceConfigMap(resourceConfigs: Seq[IOResourceConfig]): IOResourceConfigurations = {
    (for (rc <- resourceConfigs) yield Random.alphanumeric.take(6).toString -> rc).toMap
  }

  /** Create an IOResourceConf Map for each of the given files, where the file is a key and the value is the output
    * configuration
    * @param files will be the names out the output files
    * @param configuration to provide access to the program's configuration
    * @return a map of file -> IOResourceConfig
    */
  def generateDefaultIoOutputConfiguration(
      files: String*
  )(configuration: OTConfig): IOResourceConfigurations = {
    files.map { n =>
      n -> IOResourceConfig(configuration.common.outputFormat, configuration.common.output + s"/$n")
    } toMap
  }

  /** Helper function to write multiple outputs
    * @param outputs map of String and relative IOResourceg
    * @return outputs itself.
    */
  def writeTo(outputs: IOResources)(implicit session: SparkSession): IOResources = {
    val datasetNamesStr = outputs.keys.mkString("(", ", ", ")")
    logger.info(s"write datasets $datasetNamesStr")
    outputs foreach { out =>
      logger.info(s"save dataset ${out._1} with ${out._2.toString}")

      val data = out._2.data
      val conf = out._2.configuration

      val pb = conf.partitionBy.foldLeft(data.write) {
        case (df, ops) =>
          logger.debug(s"enabled partition by ${ops.toString}")
          df.partitionBy(ops: _*)
      }

      conf.options
        .foldLeft(pb) {
          case (df, ops) =>
            logger.debug(s"write to ${conf.path} with options ${ops.toString}")
            val options = ops.map(c => c.k -> c.v).toMap
            df.options(options)

        }
        .format(conf.format)
        .save(conf.path)

    }

    outputs
  }

  def makeSentencerModel(df: DataFrame, colName: String) = {
    // https://nlp.johnsnowlabs.com/docs/en/models#english---models
    val tmpCol1 = Random.alphanumeric.take(6).mkString
    val tmpCol2 = Random.alphanumeric.take(6).mkString

    val documentAssembler = new DocumentAssembler()
      .setInputCol(colName)
      .setOutputCol(tmpCol1)

    val sentencerDL = new SentenceDetectorDLApproach()
      .setInputCols(tmpCol1)
      .setOutputCol(tmpCol2)

    val pipeline = new Pipeline()
      .setStages(
        Array(
          documentAssembler,
          sentencerDL
        )
      )

    pipeline.fit(df)
  }

  def makeSentencerPipeline(fromCol: String, toCol: String): Pipeline = {
    // https://nlp.johnsnowlabs.com/docs/en/models#english---models
    val tmpCol1 = Random.alphanumeric.take(6).mkString
    val tmpCol2 = Random.alphanumeric.take(6).mkString

    val documentAssembler = new DocumentAssembler()
      .setInputCol(fromCol)
      .setOutputCol(tmpCol1)
      .setCleanupMode("shrink")

//    val sentencerDL = SentenceDetectorDLModel
//      .pretrained("sentence_detector_dl", "en")
//      .setInputCols(tmpCol1)
//      .setOutputCol(tmpCol2)

    val sentencer = new SentenceDetector()
      .setInputCols(tmpCol1)
      .setOutputCol(toCol)
      .setMinLength(5)

    val finisher = new Finisher()
      .setInputCols(toCol)
      .setIncludeMetadata(false)

    val pipeline = new Pipeline()
      .setStages(
        Array(
          documentAssembler,
//          sentencerDL,
          sentencer,
          finisher
        )
      )

    pipeline
  }

  def processSentences(df: DataFrame): DataFrame = {
    val pipeline = makeSentencerPipeline("sectionContent", "sentences")
    val annotations = pipeline
      .fit(df)
      .transform(df)

    annotations
      .drop("sectionContent")
      .withColumn("sentence", explode(col("finished_sentences")))
      .drop("finished_sentences")
  }

  def timeIt[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val delta = (t1 - t0) / 1000000000D // to secs fraction
    logger.info("Elapsed time: " + delta.toString + "secs")

    result
  }
}
