package io.opentargets.etl.literature

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import io.opentargets.etl.literature.spark.Helpers
import io.opentargets.etl.literature.spark.Helpers.IOResource
import org.apache.spark.sql._

object Embedding extends Serializable with LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Embedding step")

    val empcConfiguration = context.configuration.embedding

    val mappedInputs = Map(
      // output of Analysis step. Matches data
      "matches" -> empcConfiguration.matches
    )

    val inputDataFrames = Helpers.readFrom(mappedInputs)


    
    //val outputs = context.configuration.embedding.outputs
    //logger.info(s"write to ${context.configuration.common.output}/word2vec")
    //val dataframesToSave = Map(
   //   "word2vec" -> IOResource(...df, outputs.xxx)
    //)

    //Helpers.writeTo(dataframesToSave)
  }

}
