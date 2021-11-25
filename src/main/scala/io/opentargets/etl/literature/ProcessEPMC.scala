package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.ProcessPMC.logger
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.Try
import scala.xml._

object ProcessEPMC extends Serializable with LazyLogging {

  val colNames = Seq(
    "filename",
    "pmid",
    "pmcid",
    "date",
    "title",
    "section",
    "sectionType",
    "sectionContent"
  )

  def processEPMC(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val pubs = sparkSession.sparkContext.wholeTextFiles(path)
    val pubmed = pubs
      .flatMap(f => {
        for {
          pub <- XML.loadString(f._2) \\ "article"
        } yield {
          val pmid = pub \ "MedlineCitation" \ "PMID"
          val date = (pub \ "PubmedData" \ "History" \\ "PubMedPubDate")
            .withFilter(d => (d \@ "PubStatus") == "entrez")
            .map(date => {
              val y = Try((date \ "Year").text.toInt).getOrElse(1965)
              val m = Try((date \ "Month").text.toInt).getOrElse(1)
              val d = Try((date \ "Day").text.toInt).getOrElse(1)
              f"$y%04d-$m%02d-$d%02d"
            })
            .head
          val article = pub \ "MedlineCitation" \ "Article"
          val title = (article \ "ArticleTitle")
          val sections = {
            ("title", null, title.text) +:
              (article \ "Abstract" \\ "AbstractText").map(abs =>
              ("abstract", abs \@ "Label", abs.text))
          }
          (f._1, pmid.text, date, sections)
        }
      })
      .toDF("filename", "pmid", "date", "sections")
      .withColumn("_section", explode_outer($"sections"))
      .drop("sections")
      .withColumn("section", $"_section._1")
      .withColumn("sectionType",
                  when($"_section._2".isNotNull and ($"_section._2" !== ""),
                       lower(trim($"_section._2"))))
      .withColumn("sectionContent", $"_section._3")
      .withColumn("pmcid", typedLit[String](null))
      .withColumn("date", to_date($"date"))
      .drop("_section")
      .selectExpr(colNames: _*)

    pubmed
  }

  def apply()(implicit context: ETLSessionContext): Map[String, IOResource] = {
    implicit val ss: SparkSession = context.sparkSession
    logger.info("PreProcessing step")

    val epmcConf = context.configuration.epmc
    val epmcDF = processEPMC(epmcConf.path)

    val dataframesToSave = Map(
      "epmc" -> IOResource(epmcDF, epmcConf.output)
    )

    Helpers.writeTo(dataframesToSave)
  }

}
