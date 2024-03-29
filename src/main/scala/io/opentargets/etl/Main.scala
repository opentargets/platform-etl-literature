package io.opentargets.etl

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.literature._

object Main {

  def main(args: Array[String]): Unit =
    ETL(args.toSeq)
}

object ETL extends LazyLogging {

  def applySingleStep(step: String)(implicit context: ETLSessionContext): Unit = {
    step match {
      case "all" =>
        logger.info("run all steps")
        Processing()
        Embedding()
        Vectors()
        Evidence()
      case "processing" =>
        logger.info("run step Analysis")
        Processing()
      case "embedding" =>
        logger.info("run step Embedding")
        Embedding()
      case "vectors" =>
        logger.info("run step Vectors")
        Vectors()
      case "evidence" =>
        logger.info("run step evidence")
        Evidence()
      case _ => logger.warn(s"step $step is unknown so nothing to execute")
    }
    logger.info(s"finished to run step ($step)")
  }

  def apply(steps: Seq[String]): Unit =
    ETLSessionContext() match {
      case Right(otContext) =>
        implicit val ctxt: ETLSessionContext = otContext

        logger.debug(ctxt.configuration.toString)

        val etlSteps =
          if (steps.isEmpty) otContext.configuration.common.defaultSteps
          else steps

        val unknownSteps = etlSteps filterNot otContext.configuration.common.defaultSteps.contains
        val knownSteps = etlSteps filter otContext.configuration.common.defaultSteps.contains

        logger.info(s"valid steps to execute: $knownSteps")
        if (unknownSteps.nonEmpty) logger.warn(s"invalid steps to skip: $unknownSteps")

        knownSteps.foreach { step =>
          logger.debug(s"step to run: '$step'")
          ETL.applySingleStep(step)
        }

      case Left(ex) => logger.error(ex.prettyPrint())
    }
}
