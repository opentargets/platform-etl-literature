package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.Configuration.ProcessingSection
import org.apache.spark.sql.functions._
import io.opentargets.etl.literature.spark.Helpers
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql._
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotator._
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object Grounding extends Serializable with LazyLogging {
  // https://meta.wikimedia.org/wiki/Stop_word_list/google_stop_word_list#English
  val googleStopWords: Array[String] =
    ("about above after again against all am an and any are aren't as at be because " +
      "been before being below between both but by can't cannot could couldn't did didn't do does doesn't doing don't down " +
      "during each few for from further had hadn't has hasn't have haven't having he he'd he'll he's her here here's hers " +
      "herself him himself his how how's i'd i'll i'm i've if in into is isn't it it's its itself let's me more most mustn't " +
      "my myself no nor not of off on once only or other ought our ours ourselves out over own same shan't she she'd she'll " +
      "she's should shouldn't so some such than that that's the their theirs them themselves then there there's these they " +
      "they'd they'll they're they've this those through to too under until up very was wasn't we we'd we'll we're we've " +
      "were weren't what what's when when's where where's which while who who's whom why why's with won't would wouldn't " +
      "you you'd you'll you're you've your yours yourself yourselves").split(" ")

  val allStopWords: Array[String] = Array("a", "i") ++ googleStopWords ++ googleStopWords.map(
    _.capitalize)

  private val labelT = "LT"
  private val tokenT = "TT"

  val pipelineColumns = List(
//    "document",
//    "token",
//    "stop",
//    "clean",
    tokenT,
    labelT
  )

  private def generatePipeline(fromCol: String, columns: List[String]): Pipeline = {
    // https://nlp.johnsnowlabs.com/docs/en/models#english---models
    val documentAssembler = new DocumentAssembler()
      .setInputCol(fromCol)
      .setOutputCol("document")

    val tokenizer = new Tokenizer()
      .setSplitChars(Array("-", "/", ":", ",", ";"))
      .setInputCols("document")
      .setOutputCol("token")
      .setLazyAnnotator(true)

    val tokenizerSymbol = new Tokenizer()
      .setSplitChars(Array(":", ",", ";"))
      .setInputCols("document")
      .setOutputCol("tokenSym")
      .setLazyAnnotator(true)

    val normaliserSymbol = new Normalizer()
      .setInputCols("tokenSym")
      .setOutputCol(tokenT)
      .setLowercase(true)
      .setCleanupPatterns(Array("[^\\w\\d\\s]", "[-]", "[/]", "[,]"))
      .setLazyAnnotator(true)

    val cleaner = new StopWordsCleaner()
      .setCaseSensitive(true)
      .setStopWords(allStopWords)
      .setInputCols("token")
      .setOutputCol("stop")
      .setLazyAnnotator(true)

    val normaliser = new Normalizer()
      .setInputCols("stop")
      .setOutputCol("clean")
      .setLowercase(true)
      .setCleanupPatterns(Array("[^\\w\\d\\s]", "[-]", "[/]"))
      .setLazyAnnotator(true)

    val stemmer = new Stemmer()
      .setInputCols("clean")
      .setOutputCol(labelT)
      .setLazyAnnotator(true)

    val finisher = new Finisher()
      .setInputCols(columns: _*)
      .setIncludeMetadata(false)

    val pipeline = new Pipeline()
      .setStages(
        Array(
          documentAssembler,
          tokenizer,
          tokenizerSymbol,
          normaliserSymbol,
          cleaner,
          normaliser,
          stemmer,
          finisher
        )
      )

    pipeline
  }

  private def disambiguate(df: DataFrame,
                           labelColumnName: String,
                           keywordColumnName: String): DataFrame = {
    // prefix is used to prefix each new temp column is created in here so no clash with
    // any other already present
    val prefix = Random.alphanumeric.take(6)
    val keyC = col(keywordColumnName)
    val distinctKeywordsPerLabelPerPub = s"${prefix}_distinctKeywordsPerLabelPerPub"
    val minDistinctKeywordsPerLabelPerPubOverKeywordPerPub =
      s"${prefix}_minDistinctKeywordsPerLabelPerPubOverKeywordPerPub"
    val minDistinctKeywordsPerLabelOverKeywordOverallPubs =
      s"${prefix}_minDistinctKeywordsPerLabelOverKeywordOverallPubs"

    val keywordColumns = "type" :: keywordColumnName :: Nil
    val windowPerKeyword = Window.partitionBy(keywordColumns.map(col): _*)

    val labelColumnsPerPub = "pmid" :: "pmcid" :: "type" :: labelColumnName :: Nil
    val keywordColumnsPerPub = "pmid" :: "pmcid" :: "type" :: keywordColumnName :: Nil
    val windowPerLabelPerPub = Window.partitionBy(labelColumnsPerPub.map(col): _*)
    val windowPerKeywordPerPub = Window.partitionBy(keywordColumnsPerPub.map(col): _*)

    df.withColumn(distinctKeywordsPerLabelPerPub,
                  approx_count_distinct(keyC, 0.001).over(windowPerLabelPerPub))
      .withColumn(minDistinctKeywordsPerLabelPerPubOverKeywordPerPub,
                  min(col(distinctKeywordsPerLabelPerPub)).over(windowPerKeywordPerPub))
      .withColumn(
        minDistinctKeywordsPerLabelOverKeywordOverallPubs,
        min(col(minDistinctKeywordsPerLabelPerPubOverKeywordPerPub)).over(windowPerKeyword))
      .filter(col(minDistinctKeywordsPerLabelPerPubOverKeywordPerPub) <= col(
        minDistinctKeywordsPerLabelOverKeywordOverallPubs))
      .drop(
        minDistinctKeywordsPerLabelOverKeywordOverallPubs,
        distinctKeywordsPerLabelPerPub,
        minDistinctKeywordsPerLabelPerPubOverKeywordPerPub
      )
  }

  private def normaliseSentence(df: DataFrame,
                                pipeline: Pipeline,
                                columnNamePrefix: String,
                                columns: List[String]): DataFrame = {
    val annotations = pipeline
      .fit(df)
      .transform(df)

    val transCols = columns.map(c => {
      s"finished_$c" -> s"${columnNamePrefix}_$c"
    })

    transCols.foldLeft(annotations) { (B, p) =>
      B.withColumnRenamed(p._1, p._2)
    }
  }

  def mapEntities(entities: DataFrame,
                  luts: DataFrame,
                  pipeline: Pipeline,
                  pipelineCols: List[String])(implicit
                                              sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val labels = entities
      .withColumn("match", explode($"matches"))
      .selectExpr("*", "match.*")
      .drop("match", "matches")
      .withColumn("nLabel", Helpers.normalise($"label"))
      .withColumn(
        "textV",
        when($"type" === "DS", array(struct('nLabel.as("keyValue"), lit(labelT).as("keyType"))))
          .when(
            $"type".isInCollection(List("GP", "CD")),
            array(struct('nLabel.as("keyValue"), lit(labelT).as("keyType")),
                  struct('nLabel.as("keyValue"), lit(tokenT).as("keyType")))
          )
      )
      .withColumn("_textV", explode($"textV"))
      .withColumn("text", $"_textV".getField("keyValue"))
      .withColumn("keyType", $"_textV".getField("keyType"))
      .transform(normaliseSentence(_, pipeline, "nerTerms", pipelineCols))
      .transform(generateKeysColumn(_, "nerTerms", "labelN"))

    val scoreCN = "factor"
    val scoreC = col(scoreCN)

    val selelectedCols = "pmid" :: "pmcid" :: "type" :: "label" :: "labelN" :: "keywordId" :: scoreCN :: Nil

    logger.info("ground and take rank 1 from the mapped ones")
    val w = Window.partitionBy($"type", $"labelN").orderBy(scoreC.desc)
    val mappedLabel = labels
      .join(luts, Seq("type", "labelN"), "left_outer")
      .withColumn("isMapped", $"keywordId".isNotNull)
      .filter($"isMapped" === true)
      .select(selelectedCols.map(col): _*)
      .repartition(2048)
      .persist()

    logger.info("disambiguate after grounding")
    val persistedMappedLabels = mappedLabel
      .withColumn("rank", dense_rank().over(w))
      .filter($"rank" === 1)
      .transform(disambiguate(_, "labelN", "keywordId"))
      .select("type", "label", "labelN", "keywordId")
      .dropDuplicates("type", "label", "keywordId")

    mappedLabel.unpersist()
    persistedMappedLabels
  }

  def resolveEntities(entities: DataFrame, mappedLabels: DataFrame)(
      implicit
      sparkSession: SparkSession): Map[String, DataFrame] = {
    import sparkSession.implicits._

    logger.info("resolve matches and cooccurrences with the grounded and filtered labels")
    val baseCols = List(
      $"pmid",
      $"pmcid",
      $"pubDate",
      $"date",
      $"year",
      $"month",
      $"day",
      $"organisms",
      $"section",
      $"text",
      $"trace_source"
    )

    val matchesCols = baseCols ::: $"labelN" :: $"match" :: Nil

    val mergedMatches = entities
      .withColumn("match", explode($"matches"))
      .drop("matches")
      .selectExpr("*", "match.*")
      .drop("match")
      .join(mappedLabels, Seq("type", "label"), "left_outer")
      .withColumn("isMapped", $"keywordId".isNotNull)
      .withColumn(
        "match",
        struct(
          $"endInSentence",
          $"label",
          $"sectionEnd",
          $"sectionStart",
          $"startInSentence",
          $"type",
          $"keywordId",
          $"isMapped"
        )
      )
      .select(matchesCols: _*)

    val mergedCooc = entities
      .withColumn("cooc", explode($"co-occurrence"))
      .drop("co-occurrence")
      .selectExpr("*", "cooc.*")
      .drop("cooc")
      .withColumn("type1", substring_index($"type", "-", 1))
      .withColumn("type2", substring_index($"type", "-", -1))
      .drop("type")
      .join(mappedLabels, $"type1" === $"type" and $"label1" === $"label", "left_outer")
      .withColumnRenamed("keywordId", "keywordId1")
      .drop("type", "label")
      .join(mappedLabels, $"type2" === $"type" and $"label2" === $"label", "left_outer")
      .withColumnRenamed("keywordId", "keywordId2")
      .drop("type", "label")
      .withColumn("isMapped", $"keywordId1".isNotNull and $"keywordId2".isNotNull)
      .withColumn(
        "co-occurrence",
        struct(
          $"association",
          $"end1",
          $"end2",
          $"sent_evidence_score".as("evidence_score"),
          $"label1",
          $"keywordId1",
          $"label2",
          $"keywordId2",
          $"relation",
          $"start1",
          $"start2",
          concat_ws("-", $"type1", $"type2").as("type"),
          $"type1",
          $"type2",
          $"isMapped"
        )
      )
      .select(baseCols :+ $"co-occurrence": _*)

    Map(
      "matches" -> mergedMatches,
      "cooccurrences" -> mergedCooc
    )
  }

  def loadEntities(df: DataFrame, epmcids: DataFrame)(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    df.withColumn("trace_source", input_file_name())
      .withColumn("pmid", when($"pmid".isNotNull and $"pmid" =!= "" and $"pmid" =!= "0", $"pmid"))
      .withColumn("pmcid",
                  when($"pmcid".isNotNull and $"pmcid" =!= "" and $"pmcid" =!= "0", $"pmcid"))
      .withColumn("failed_pmid", $"pmid".isNull)
      .withColumn("failed_pmcid", $"pmcid".isNull)
      .withColumn("failed_pmcid_and_pmid", $"pmcid".isNull and $"pmid".isNull)
      .withColumn("failed_pmid_not_pmcid", $"pmid".isNull and $"pmcid".isNotNull)
      .join(epmcids, $"pmcid" === $"pmcid_lut", "left_outer")
      .withColumn("pmid", coalesce($"pmid", $"pmid_lut"))
      .drop(epmcids.columns.filter(_.endsWith("_lut")): _*)
      .withColumn("failed_recover_pmid_not_pmcid", $"failed_pmid_not_pmcid" and $"pmid".isNotNull)
      .withColumn("date",
                  when($"pubDate".isNotNull and $"pubDate" =!= "", $"pubDate".cast(DateType)))
      .withColumn("failed_date", $"date".isNull)
      .withColumn("year", when($"date".isNotNull, year($"date")))
      .withColumn("month", when($"date".isNotNull, month($"date")))
      .withColumn("day", when($"date".isNotNull, dayofmonth($"date")))
      .withColumn("sentence", explode($"sentences"))
      .drop("sentences")
      .selectExpr("*", "sentence.*")
      .drop("sentence")
      .withColumn("section", lower($"section"))
      .withColumn("failed_section", $"section".isNull)
      .withColumn("failed_sentence", $"text".rlike("[^\\x20-\\x7e]"))
  }

  def sampleEntities(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    // check for pmcid null
    // check for pmid null
    // check for section null
    // sample 1% 0.01

    val fileCol = "trace_source"
    val failedCols = df.columns.filter(_.startsWith("failed_"))
    val okCols = failedCols.map(c => col(c) === true)
    val okFilter = okCols.reduceLeft((B, c) => B.or(c))

    val allFailedCols = failedCols :+ "failed_true" :+ "failed_false"
    val aggs = allFailedCols.map { cn =>
      sum(when(col(cn) === true, lit(1)).otherwise(0)).as(cn + "_count")
    } ++ Array(
      collect_set(when($"failed_recover_pmid_not_pmcid" === true, struct($"pmcid", $"pmid")))
        .as("failed_recovered_pmcids"))

    val r = df
      .withColumn("failed_true", when(okFilter, typedLit(true)).otherwise(typedLit(false)))
      .withColumn("failed_false", not($"failed_true"))

    r.groupBy(col(fileCol))
      .agg(aggs.head, aggs.tail: _*)
  }

  def filterEntities(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val failedColumns = df.columns.filter(_.startsWith("failed_"))

    df.drop(failedColumns: _*)
      .filter($"pmid".isNotNull)
      .filter($"section".isNotNull)
  }

  private def cleanAndScoreArrayColumn[A](c: Column, score: Double, keyTypeName: String): Column =
    transform(coalesce(c, array()),
              c => struct(c.as("key"), lit(score).as("factor"), lit(keyTypeName).as("keyType")))

  private def generateKeysColumn(df: DataFrame, columnPrefix: String, keyColumnName: String)(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val labelColumn = s"${columnPrefix}_$labelT"
    val tokenColumn = s"${columnPrefix}_$tokenT"

    df.withColumn(
        keyColumnName,
        when($"keyType" === labelT,
             array_join(
               array_sort(filter(array_distinct(col(labelColumn)), c => c.isNotNull and c =!= "")),
               ""))
          .when($"keyType" === tokenT,
                array_join(filter(col(tokenColumn), c => c.isNotNull and c =!= ""), ""))
      )
      .filter(col(keyColumnName).isNotNull and length(col(keyColumnName)) > 0)
  }

  private def transformDiseases(
      diseases: DataFrame,
      pipeline: Pipeline,
      pipelineCols: List[String])(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    diseases
      .selectExpr("id as keywordId", "name", "synonyms.*")
      .withColumn("nameC", cleanAndScoreArrayColumn[String](array($"name"), 1D, labelT))
      .withColumn("exactSynonyms",
                  cleanAndScoreArrayColumn[String]($"hasExactSynonym", 0.999, labelT))
      .withColumn("narrowSynonyms",
                  cleanAndScoreArrayColumn[String]($"hasNarrowSynonym", 0.998, labelT))
      .withColumn("broadSynonyms",
                  cleanAndScoreArrayColumn[String]($"hasBroadSynonym", 0.997, labelT))
      .withColumn("relatedSynonyms",
                  cleanAndScoreArrayColumn[String]($"hasRelatedSynonym", 0.996, labelT))
      .withColumn(
        "_text",
        explode(
          flatten(
            array($"nameC",
                  $"broadSynonyms",
                  $"exactSynonyms",
                  $"narrowSynonyms",
                  $"relatedSynonyms")))
      )
      .withColumn("text", $"_text".getField("key"))
      .withColumn("factor", $"_text".getField("factor"))
      .withColumn("keyType", $"_text".getField("keyType"))
      .select("keywordId", "text", "factor", "keyType")
      .filter($"text".isNotNull and length($"text") > 0)
      .transform(normaliseSentence(_, pipeline, "efoTerms", pipelineCols))
      .transform(generateKeysColumn(_, "efoTerms", "key"))
  }

  private def transformTargets(targets: DataFrame, pipeline: Pipeline, pipelineCols: List[String])(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    targets
      .select(
        $"id" as "keywordId",
        $"approvedName" as "name",
        $"approvedSymbol" as "symbol",
        $"symbolSynonyms.label" as "symbolSynonyms",
        $"nameSynonyms.label" as "nameSynonyms",
        $"obsoleteSymbols.label" as "obsoleteSymbols",
        $"obsoleteNames.label" as "obsoleteNames",
        array_distinct(coalesce($"proteinIds.id", typedLit(Array.empty[String]))) as "accessions"
      )
      .withColumn("nameC", cleanAndScoreArrayColumn[String](array($"name"), 1, labelT))
      .withColumn("symbolC", cleanAndScoreArrayColumn[String](array($"symbol"), 1, tokenT))
      .withColumn("nameSynonymsC", cleanAndScoreArrayColumn[String]($"nameSynonyms", 0.999, labelT))
      .withColumn("symbolSynonymsC",
                  cleanAndScoreArrayColumn[String]($"symbolSynonyms", 0.999, tokenT))
      .withColumn("accessionsC", cleanAndScoreArrayColumn[String]($"accessions", 0.999, tokenT))
      .withColumn("obsoleteNamesC",
                  cleanAndScoreArrayColumn[String]($"obsoleteNames", 0.998, labelT))
      .withColumn("obsoleteSymbolsC",
                  cleanAndScoreArrayColumn[String]($"obsoleteSymbols", 0.998, tokenT))
      .withColumn(
        "_text",
        explode(
          filter(
            array_distinct(
              flatten(
                array(
                  $"nameC",
                  $"symbolC",
                  $"nameSynonymsC",
                  $"symbolSynonymsC",
                  $"obsoleteNamesC",
                  $"obsoleteSymbolsC",
                  $"accessionsC"
                )
              )
            ),
            c => length(c.getField("key")) > 0
          )
        )
      )
      .withColumn("text", $"_text".getField("key"))
      .withColumn("factor", $"_text".getField("factor"))
      .withColumn("keyType", $"_text".getField("keyType"))
      .select("keywordId", "text", "factor", "keyType")
      .filter($"text".isNotNull and length($"text") > 0)
      .transform(normaliseSentence(_, pipeline, "targetTerms", pipelineCols))
      .transform(generateKeysColumn(_, "targetTerms", "key"))
  }

  private def transformDrugs(drugs: DataFrame, pipeline: Pipeline, pipelineCols: List[String])(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    drugs
      .selectExpr("id as keywordId", "name", "tradeNames", "synonyms")
      .withColumn("nameL", cleanAndScoreArrayColumn[String](array($"name"), 1, labelT))
      .withColumn("nameT", cleanAndScoreArrayColumn[String](array($"name"), 1, tokenT))
      .withColumn("tradeNamesL", cleanAndScoreArrayColumn[String]($"tradeNames", 0.999, labelT))
      .withColumn("tradeNamesT", cleanAndScoreArrayColumn[String]($"tradeNames", 0.999, tokenT))
      .withColumn("synonymsL", cleanAndScoreArrayColumn[String]($"synonyms", 0.999, labelT))
      .withColumn("synonymsT", cleanAndScoreArrayColumn[String]($"synonyms", 0.999, tokenT))
      .withColumn(
        "_text",
        explode(flatten(
          array($"nameL", $"nameT", $"tradeNamesL", $"tradeNamesT", $"synonymsL", $"synonymsT"))))
      .withColumn("text", $"_text".getField("key"))
      .withColumn("factor", $"_text".getField("factor"))
      .withColumn("keyType", $"_text".getField("keyType"))
      .select("keywordId", "text", "factor", "keyType")
      .filter($"text".isNotNull and length($"text") > 0)
      .transform(normaliseSentence(_, pipeline, "drugTerms", pipelineCols))
      .transform(generateKeysColumn(_, "drugTerms", "key"))
  }

  def loadEntityLUT(
      targets: DataFrame,
      diseases: DataFrame,
      drugs: DataFrame,
      pipeline: Pipeline,
      pipelineColumns: List[String])(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val cols = List(
      "key as labelN",
      "type",
      "keywordId",
      "factor"
    )

    val D = transformDiseases(diseases, pipeline, pipelineColumns)
      .withColumn("type", lit("DS"))
      .selectExpr(cols: _*)

    val T = transformTargets(targets, pipeline, pipelineColumns)
      .withColumn("type", lit("GP"))
      .selectExpr(cols: _*)

    val DR = transformDrugs(drugs, pipeline, pipelineColumns)
      .withColumn("type", lit("CD"))
      .selectExpr(cols: _*)

    val lut = D
      .unionByName(T)
      .unionByName(DR)
      .distinct()
      .orderBy($"type", $"labelN")

    lut
  }

  def loadEPMCIDs(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    df.selectExpr("PMID as pmid_lut", "PMCID as pmcid_lut")
      .filter($"pmcid_lut".isNotNull and $"pmid_lut".isNotNull and $"pmcid_lut".startsWith("PMC"))
      .orderBy($"pmcid_lut")
  }

  def compute(empcConfiguration: ProcessingSection)(
      implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Grounding step")

    val pipeline = generatePipeline("text", pipelineColumns)

    val mappedInputs = Map(
      // search output of ETL. (disease,drug,target)
      "epmcids" -> empcConfiguration.epmcids,
      "targets" -> empcConfiguration.targets,
      "diseases" -> empcConfiguration.diseases,
      "drugs" -> empcConfiguration.drugs,
      "epmc" -> empcConfiguration.epmc
    )

    val inputDataFrames = Helpers.readFrom(mappedInputs)

    logger.info("Load PMCID-PMID lut and OT entity lut")
    val idLUT = broadcast(loadEPMCIDs(inputDataFrames("epmcids").data))
    val luts = broadcast(
      loadEntityLUT(
        inputDataFrames("targets").data,
        inputDataFrames("diseases").data,
        inputDataFrames("drugs").data,
        pipeline,
        pipelineColumns
      ))

    val epmcDf = Helpers.replaceSpacesSchema(inputDataFrames("epmc").data)

    logger.info("load and preprocess EPMC data")
    val entities = loadEntities(epmcDf, idLUT)

    val sampledDF = entities.transform(sampleEntities)
    val sentences = entities.transform(filterEntities)
    val mappedLabels =
      mapEntities(sentences, luts, pipeline, pipelineColumns)

    logger.info("producing grounding dataset")
    Helpers.writeTo(
      Map("mappedLabels" -> IOResource(mappedLabels, empcConfiguration.outputs.grounding)))

    val aux = Helpers.readFrom(Map("mappedLabels" -> empcConfiguration.outputs.grounding))
    val mappedDF = aux("mappedLabels").data.orderBy(col("type"), col("label"))

    logger.info("resolve entities with the produced grounded labels")
    val resolvedEntities = resolveEntities(sentences, mappedDF)

    resolvedEntities + ("samples" -> sampledDF)
  }
}
