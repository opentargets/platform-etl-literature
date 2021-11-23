package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.storage.StorageLevel

import scala.xml._

object PreProcessing extends Serializable with LazyLogging {
  def initial(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val pubs = sparkSession.sparkContext.wholeTextFiles(path)
    val pubmed = pubs
      .flatMap(f => {
        for {
          pub <- XML.loadString(f._2) \\ "PubmedArticle"
        } yield {
          val pmid = pub \ "MedlineCitation" \ "PMID"
          val date = (pub \ "PubmedData" \ "History" \\ "PubMedPubDate")
            .withFilter(d => (d \@ "PubStatus") == "entrez")
            .map(_.text)
            .headOption
          val article = pub \ "MedlineCitation" \ "Article"
          val title = article \ "ArticleTitle"
          val abstracts =
            (article \ "Abstract" \\ "AbstractText").map(abs => (abs \@ "Label", abs.text))
          (f._1, pmid.text, date, title.text, abstracts)
        }
      })
      .toDF("filename", "pmid", "date", "title", "abstracts")

    pubmed
      .withColumn("abstract", explode_outer($"abstracts"))
      .drop("abstracts")
      .withColumn("section", lit("abstract"))
      .withColumn("sectionType",
                  when($"abstract._1".isNotNull and ($"abstract._1" !== ""),
                       lower(trim($"abstract._1"))))
      .withColumn("sectionContent", $"abstract._2")
      .drop("abstract")

  }

  def apply()(implicit context: ETLSessionContext): Map[String, IOResource] = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("PreProcessing step")
    initial()
  }

}
