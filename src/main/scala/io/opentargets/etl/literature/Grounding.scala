package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.Configuration.ProcessingSection
import org.apache.spark.sql.functions._
import io.opentargets.etl.literature.spark.Helpers
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql._
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher, SparkNLP}
import com.johnsnowlabs.nlp.annotator._
import io.opentargets.etl.literature.spark.Helpers.{IOResource, IOResourceConfig}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType}

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

  val pipelineColumns = List(
    "document",
    "token",
    "stop",
    "clean",
    "stem"
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
      .setOutputCol("stem")
      .setLazyAnnotator(true)

    val finisher = new Finisher()
      .setInputCols(columns: _*)
      .setIncludeMetadata(false)

    val pipeline = new Pipeline()
      .setStages(
        Array(
          documentAssembler,
          tokenizer,
          cleaner,
          normaliser,
          stemmer,
          finisher
        )
      )

    pipeline
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

  def resolveEntities(entities: DataFrame,
                      luts: DataFrame,
                      pipeline: Pipeline,
                      pipelineCols: List[String])(
      implicit
      sparkSession: SparkSession): Map[String, DataFrame] = {
    import sparkSession.implicits._

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
      $"text"
    )

    val mergedMatches = entities
      .withColumn("match", explode($"matches"))
      .drop("matches")
      .selectExpr("*", "match.*")
      .drop("match")
      .withColumnRenamed("text", "_text")
      .withColumn("text", Helpers.normalise($"label"))
      .transform(normaliseSentence(_, pipeline, "nerTerms", pipelineCols))
      .withColumn("labelN",
                  array_join(transform(array_sort(array_distinct($"nerTerms_stem")), lower _), "-"))
      .filter($"labelN".isNotNull and length($"labelN") > 0)
      .drop("text")
      .withColumnRenamed("_text", "text")
      .join(luts, Seq("type", "labelN"), "left_outer")
      .withColumn("isMapped", $"keywordId".isNotNull)
      .withColumn("match",
                  struct(
                    $"endInSentence",
                    $"label",
                    $"sectionEnd",
                    $"sectionStart",
                    $"startInSentence",
                    $"type",
                    $"keywordId",
                    $"isMapped"
                  ))
      .select(baseCols :+ $"match": _*)

    val mergedCooc = entities
      .withColumn("cooc", explode($"co-occurrence"))
      .drop("co-occurrence")
      .selectExpr("*", "cooc.*")
      .drop("cooc")
      .withColumn("label1N", Helpers.normalise($"label1"))
      .withColumnRenamed("text", "_text")
      .withColumn("text", Helpers.normalise($"label1"))
      .transform(normaliseSentence(_, pipeline, "nerTerms1", pipelineCols))
      .withColumn("label1N",
                  array_join(transform(array_sort(array_distinct($"nerTerms1_stem")), lower _),
                             "-"))
      .withColumn("text", Helpers.normalise($"label2"))
      .transform(normaliseSentence(_, pipeline, "nerTerms2", pipelineCols))
      .withColumn("label2N",
                  array_join(transform(array_sort(array_distinct($"nerTerms2_stem")), lower _),
                             "-"))
      .drop("text")
      .withColumnRenamed("_text", "text")
      .withColumn("type1", substring_index($"type", "-", 1))
      .withColumn("type2", substring_index($"type", "-", -1))
      .drop("type")
      .join(luts, $"type1" === $"type" and $"label1N" === $"labelN", "left_outer")
      .withColumnRenamed("keywordId", "keywordId1")
      .drop("type", "labelN")
      .join(luts, $"type2" === $"type" and $"label2N" === $"labelN", "left_outer")
      .withColumnRenamed("keywordId", "keywordId2")
      .drop("type", "labelN")
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

  def foldCooccurrences(df: DataFrame)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    df.groupBy($"pmid", $"section", $"text")
      .agg(
        first($"pubDate").as("pubDate"),
        first($"organisms").as("organisms"),
        collect_set($"co-occurrence").as("co-occurrence")
      )
      .filter($"co-occurrence".isNotNull and size($"co-occurrence") > 0)
      .groupBy($"pmid")
      .agg(
        first($"pubDate").as("pubDate"),
        first($"organisms").as("organisms"),
        collect_list(struct($"text", $"section", $"co-occurrence")).as("sentences")
      )
      .filter($"sentences".isNotNull and size($"sentences") > 0)
  }

  def foldMatches(df: DataFrame)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    df.groupBy($"pmid", $"section", $"text")
      .agg(
        first($"pubDate").as("pubDate"),
        first($"organisms").as("organisms"),
        collect_set($"match").as("matches")
      )
      .filter($"matches".isNotNull and size($"matches") > 0)
      .groupBy($"pmid")
      .agg(
        first($"pubDate").as("pubDate"),
        first($"organisms").as("organisms"),
        collect_list(struct($"text", $"section", $"matches")).as("sentences")
      )
      .filter($"sentences".isNotNull and size($"sentences") > 0)
  }

  def loadEntities(df: DataFrame, epmcids: DataFrame)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val hammerDateField = udf((psuedoDate: String) =>
      psuedoDate.split("/").map(_.toInt).toList match {
        case day :: month :: year :: Nil if year > 1600 =>
          Some(f"$year%04d-$month%02d-$day%02d")
        case _ => None
    })

    df.withColumn("sentence", explode($"sentences"))
      .drop("sentences")
      .withColumn("pmid", when($"pmid" =!= "", $"pmid"))
      .withColumn("pmcid", when($"pmcid" =!= "", $"pmcid"))
      .join(epmcids, $"pmcid" === $"pmcid_lut", "left_outer")
      .withColumn("pmid", coalesce($"pmid", $"pmid_lut").cast(IntegerType))
      .drop(epmcids.columns.filter(_.endsWith("_lut")): _*)
      .filter($"pmid".isNotNull)
      .selectExpr("*", "sentence.*")
      .drop("sentence")
      .withColumn("section", lower($"section"))
      .filter($"section".isNotNull)
      .withColumn("date", when($"pubDate" =!= "", hammerDateField($"pubDate").cast(DateType)))
      .withColumn("year", when($"date".isNotNull, year($"date")))
      .withColumn("month", when($"date".isNotNull, month($"date")))
      .withColumn("day", when($"date".isNotNull, dayofmonth($"date")))
  }

  private def transformDiseases(
      diseases: DataFrame,
      pipeline: Pipeline,
      pipelineCols: List[String])(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    diseases
      .selectExpr("id as keywordId", "name", "synonyms.*")
      .withColumn("exactSynonyms",
                  transform(expr("coalesce(hasExactSynonym, array())"),
                            c => struct(c.as("key"), lit(0.999).as("factor"))))
      .withColumn("narrowSynonyms",
                  transform(expr("coalesce(hasNarrowSynonym, array())"),
                            c => struct(c.as("key"), lit(0.998).as("factor"))))
      .withColumn("broadSynonyms",
                  transform(expr("coalesce(hasBroadSynonym, array())"),
                            c => struct(c.as("key"), lit(0.997).as("factor"))))
      .withColumn("relatedSynonyms",
                  transform(expr("coalesce(hasRelatedSynonym, array())"),
                            c => struct(c.as("key"), lit(0.996).as("factor"))))
      .withColumn(
        "_text",
        explode(
          flatten(
            array(array(struct($"name".as("key"), lit(1d).as("factor"))),
                  $"broadSynonyms",
                  $"exactSynonyms",
                  $"narrowSynonyms",
                  $"relatedSynonyms")))
      )
      .withColumn("text", $"_text".getField("key"))
      .withColumn("factor", $"_text".getField("factor"))
      .select("keywordId", "text", "factor")
      .filter($"text".isNotNull and length($"text") > 0)
      .transform(normaliseSentence(_, pipeline, "efoTerms", pipelineCols))
      .withColumn("key",
                  array_join(transform(array_sort(array_distinct($"efoTerms_stem")), lower _), "-"))
      .filter($"key".isNotNull and length($"key") > 0)
  }

  private def transformTargets(targets: DataFrame, pipeline: Pipeline, pipelineCols: List[String])(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    targets
      .selectExpr(
        "id as keywordId",
        "approvedName as name",
        "approvedSymbol as symbol",
        "coalesce(nameSynonyms, array()) as nameSynonyms",
        "coalesce(symbolSynonyms, array()) as symbolSynonyms",
        "coalesce(proteinAnnotations.accessions, array()) as accessions"
      )
      .withColumn("nameSynonyms",
                  transform($"nameSynonyms", c => struct(c.as("key"), lit(0.999).as("factor"))))
      .withColumn("symbolSynonyms",
                  transform($"symbolSynonyms", c => struct(c.as("key"), lit(0.999).as("factor"))))
      .withColumn("accessions",
                  transform($"accessions", c => struct(c.as("key"), lit(1D).as("factor"))))
      .withColumn(
        "_text",
        explode(flatten(array(
          array(struct($"name".as("key"), lit(1d).as("factor"))),
          array(struct($"symbol".as("key"), lit(1d).as("factor"))),
          $"nameSynonyms",
          $"symbolSynonyms",
          $"accessions"
        )))
      )
      .withColumn("text", $"_text".getField("key"))
      .withColumn("factor", $"_text".getField("factor"))
      .select("keywordId", "text", "factor")
      .filter($"text".isNotNull and length($"text") > 0)
      .transform(normaliseSentence(_, pipeline, "targetTerms", pipelineCols))
      .withColumn(
        "key",
        array_join(transform(array_sort(array_distinct($"targetTerms_stem")), lower _), "-"))
      .filter($"key".isNotNull and length($"key") > 0)
  }

  private def transformDrugs(drugs: DataFrame, pipeline: Pipeline, pipelineCols: List[String])(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    drugs
      .selectExpr("id as keywordId", "name", "tradeNames", "synonyms")
      .withColumn("tradeNames",
                  transform(expr("coalesce(tradeNames, array())"),
                            c => struct(c.as("key"), lit(1D).as("factor"))))
      .withColumn("synonyms",
                  transform(expr("coalesce(synonyms, array())"),
                            c => struct(c.as("key"), lit(0.999).as("factor"))))
      .withColumn(
        "_text",
        explode(
          flatten(array(array(struct($"name".as("key"), lit(1d).as("factor"))),
                        $"tradeNames",
                        $"synonyms")))
      )
      .withColumn("text", $"_text".getField("key"))
      .withColumn("factor", $"_text".getField("factor"))
      .select("keywordId", "text", "factor")
      .filter($"text".isNotNull and length($"text") > 0)
      .transform(normaliseSentence(_, pipeline, "drugTerms", pipelineCols))
      .withColumn(
        "key",
        array_join(transform(array_sort(array_distinct($"drugTerms_stem")), lower _), "-"))
      .filter($"key".isNotNull and length($"key") > 0)
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
      "keywordId"
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
      .orderBy($"labelN")
      .distinct()

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

    val resolvedEntities = resolveEntities(entities, luts, pipeline, pipelineColumns)

    resolvedEntities
  }
}
