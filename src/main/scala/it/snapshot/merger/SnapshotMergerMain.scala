package it.snapshot.merger

import com.typesafe.scalalogging.LazyLogging
import it.snapshot.merger.controller.MergeController
import it.snapshot.merger.mongo.MongoHelper
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.HashMap
import scala.util.{Failure, Success, Try}

object SnapshotMergerMain extends LazyLogging{
  def main(args: Array[String]): Unit = {
    logger.info("Starting the snapshot merging process...")
    val spark = SparkSession.builder().appName("Snapshot Merger").enableHiveSupport().getOrCreate()
    val inputs = parseInputs(args)
    val mergeJobs = MongoHelper.getMergeJobs(inputs)
    MongoHelper.initMergeJobTracer(spark.sparkContext.applicationId, mergeJobs.size)
    var totalCounter = 0
    var completedCounter = 0
    logger.info(s"${mergeJobs.size} tables found.")
    for (mergeJob <- mergeJobs) {
      val trySc = Try {
        new MergeController(mergeJob, spark, inputs)
      }
      trySc match {
        case Success(mergeController) =>
          mergeController.manageMergeUpdate()
          totalCounter += mergeController.totalCounter
          completedCounter += mergeController.completedCounter
        case Failure(e) => logger.warn(s"Error during the MergeController initialization: ${e.getMessage}." +
          s" Trying the next table...")
      }
    }
    MongoHelper.closeJobTracer(completedCounter, totalCounter)
    logger.info("Merge process completed.")
  }

  def parseInputs(args: Array[String]): HashMap[String, String] = {
    var database = ""
    var table = ""
    var startDate = ""
    var endDate = ""
    var maxDays = ""
    for (i <- args.indices) {
      if (args(i) == "--database" && i + 1 < args.length)
        database = args(i + 1)
      else if (args(i) == "--res" && i + 1 < args.length)
        table = args(i + 1)
      else if (args(i) == "--startDate" && i + 1 < args.length)
        startDate = args(i + 1)
      else if (args(i) == "--endDate" && i + 1 < args.length)
        endDate = args(i + 1)
      else if (args(i) == "--maxDays" && i + 1 < args.length)
        maxDays = args(i + 1)
    }
    HashMap("database" -> database, "table" -> table, "startDate" -> startDate, "endDate" -> endDate, "maxDays" -> maxDays)
  }
}
